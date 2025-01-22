/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.impl.GenericDocumentImpl;
import com.marklogic.client.io.Format;
import com.marklogic.spark.*;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.TripleRowSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class WriteContext extends ContextSupport {

    static final long serialVersionUID = 1;

    private final StructType schema;
    private final boolean usingFileSchema;
    private final int batchSize;

    private int fileSchemaContentPosition;
    private int fileSchemaPathPosition;

    // This unfortunately is not final as we don't know it when this object is created.
    private int numPartitions;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;
        this.batchSize = getIntOption(Options.WRITE_BATCH_SIZE, 100, 1);

        // We support the Spark binaryFile schema - https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html -
        // so that reader can be reused for loading files as-is.
        final List<String> names = Arrays.asList(this.schema.fieldNames());
        this.usingFileSchema = names.size() == 4 && names.contains("path") && names.contains("content")
            && names.contains("modificationTime") && names.contains("length");
        if (this.usingFileSchema) {
            // Per the Spark docs, we expect positions 0 and 3 here, but looking them up just to be safe.
            this.fileSchemaPathPosition = names.indexOf("path");
            this.fileSchemaContentPosition = names.indexOf("content");
        }
    }

    public StructType getSchema() {
        return schema;
    }

    /**
     * @return the total number of threads to use across all partitions. This is typically how a user thinks in terms
     * of, as they are not likely to know how many partitions will be created. But they will typically know how many
     * hosts are in their MarkLogic cluster and how many threads are available to an app server on each host.
     */
    int getTotalThreadCount() {
        return getIntOption(Options.WRITE_THREAD_COUNT, 4, 1);
    }

    /**
     * @return the thread count to use per partition where a user has specified the total thread count across all
     * partitions.
     */
    int getCalculatedThreadCountPerPartition() {
        int threadCount = getTotalThreadCount();
        if (this.numPartitions > 0) {
            return (int) Math.ceil((double) threadCount / (double) numPartitions);
        }
        return threadCount;
    }

    /**
     * @return the thread count to use per partition where a user has used an option to explicitly define how many
     * threads should be used by a partition.
     */
    int getUserDefinedThreadCountPerPartition() {
        return getIntOption(Options.WRITE_THREAD_COUNT_PER_PARTITION, 0, 1);
    }

    WriteBatcher newWriteBatcher(DataMovementManager dataMovementManager) {
        // If the user told us how many threads they want per partition (we expect this to be rare), then use that.
        // Otherwise, use the calculated number of threads per partition based on the total thread count that either
        // the user configured or using the default value for that option.
        final int threadCount = getUserDefinedThreadCountPerPartition() > 0 ?
            getUserDefinedThreadCountPerPartition() : getCalculatedThreadCountPerPartition();

        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Creating new batcher with thread count of {} and batch size of {}.", threadCount, batchSize);
        }
        WriteBatcher writeBatcher = dataMovementManager
            .newWriteBatcher()
            .withBatchSize(batchSize)
            .withThreadCount(threadCount)
            .withTemporalCollection(getStringOption(Options.WRITE_TEMPORAL_COLLECTION))
            .onBatchSuccess(this::logBatchOnSuccess);

        Optional<ServerTransform> transform = makeRestTransform();
        if (transform.isPresent()) {
            writeBatcher.withTransform(transform.get());
        }
        return writeBatcher;
    }

    /**
     * @param client
     * @return a {@code GenericDocumentImpl}, which exposes the methods that accept a temporal collection as an input.
     * Has the same configuration as the {@code WriteBatcher} created by this class as well, thus allowing for documents
     * in a failed batch to be retried via this document manager.
     */
    GenericDocumentImpl newDocumentManager(DatabaseClient client) {
        GenericDocumentManager mgr = client.newDocumentManager();
        Optional<ServerTransform> transform = makeRestTransform();
        if (transform.isPresent()) {
            mgr.setWriteTransform(transform.get());
        }
        return (GenericDocumentImpl) mgr;
    }

    DocBuilder newDocBuilder() {
        DocBuilderFactory factory = new DocBuilderFactory()
            .withCollections(getProperties().get(Options.WRITE_COLLECTIONS))
            .withPermissions(getProperties().get(Options.WRITE_PERMISSIONS));

        if (hasOption(Options.WRITE_URI_TEMPLATE)) {
            configureTemplateUriMaker(factory);
        } else {
            configureStandardUriMaker(factory);
        }

        return factory.newDocBuilder();
    }

    Format getDocumentFormat() {
        if (hasOption(Options.WRITE_DOCUMENT_TYPE)) {
            String value = getStringOption(Options.WRITE_DOCUMENT_TYPE);
            try {
                return Format.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                String message = "Invalid value for %s: %s; must be one of 'JSON', 'XML', or 'TEXT'.";
                String optionAlias = getOptionNameForMessage(Options.WRITE_DOCUMENT_TYPE);
                throw new ConnectorException(String.format(message, optionAlias, value));
            }
        }
        return null;
    }

    /**
     * @deprecated since 2.3.0; users should use getDocumentFormat instead.
     */
    @Deprecated(since = "2.3.0")
    // We don't need Sonar to remind us of this deprecation.
    @SuppressWarnings("java:S1133")
    Format getDeprecatedFileRowsDocumentFormat() {
        final String deprecatedOption = Options.WRITE_FILE_ROWS_DOCUMENT_TYPE;
        if (hasOption(deprecatedOption)) {
            String value = getStringOption(deprecatedOption);
            try {
                return Format.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                String message = "Invalid value for %s: %s; must be one of 'JSON', 'XML', or 'TEXT'.";
                String optionAlias = getOptionNameForMessage(deprecatedOption);
                throw new ConnectorException(String.format(message, optionAlias, value));
            }
        }
        return null;
    }

    /**
     * The URI template approach will typically be used with rows with an "arbitrary" schema where each column value
     * may be useful in constructing a URI.
     *
     * @param factory
     */
    private void configureTemplateUriMaker(DocBuilderFactory factory) {
        String uriTemplate = getProperties().get(Options.WRITE_URI_TEMPLATE);
        String optionAlias = getOptionNameForMessage(Options.WRITE_URI_TEMPLATE);
        factory.withUriMaker(new SparkRowUriMaker(uriTemplate, optionAlias));
        Stream.of(Options.WRITE_URI_PREFIX, Options.WRITE_URI_SUFFIX, Options.WRITE_URI_REPLACE).forEach(option -> {
            String value = getProperties().get(option);
            if (value != null && value.trim().length() > 0) {
                Util.MAIN_LOGGER.warn("Option {} will be ignored since option {} was specified.", option, Options.WRITE_URI_TEMPLATE);
            }
        });
    }

    /**
     * For rows with an "arbitrary" schema, the URI suffix defaults to ".json" or ".xml" as we know there won't be an
     * initial URI for these rows.
     *
     * @param factory
     */
    private void configureStandardUriMaker(DocBuilderFactory factory) {
        String uriSuffix = null;
        if (hasOption(Options.WRITE_URI_SUFFIX)) {
            uriSuffix = getProperties().get(Options.WRITE_URI_SUFFIX);
        } else if (!isUsingFileSchema() && !DocumentRowSchema.SCHEMA.equals(this.schema) && !DocumentRowSchema.IMPORT_SCHEMA.equals(this.schema) && !TripleRowSchema.SCHEMA.equals(this.schema)) {
            String xmlRootName = getStringOption(Options.WRITE_XML_ROOT_NAME);
            if (xmlRootName != null && getStringOption(Options.WRITE_JSON_ROOT_NAME) != null) {
                throw new ConnectorException(String.format("Cannot specify both %s and %s",
                    getOptionNameForMessage(Options.WRITE_JSON_ROOT_NAME), getOptionNameForMessage(Options.WRITE_XML_ROOT_NAME)));
            }
            uriSuffix = xmlRootName != null ? ".xml" : ".json";
        }
        factory.withUriMaker(new StandardUriMaker(
            getProperties().get(Options.WRITE_URI_PREFIX), uriSuffix,
            getProperties().get(Options.WRITE_URI_REPLACE)
        ));
    }

    private Optional<ServerTransform> makeRestTransform() {
        String transformName = getProperties().get(Options.WRITE_TRANSFORM_NAME);
        if (transformName != null && transformName.trim().length() > 0) {
            ServerTransform transform = new ServerTransform(transformName);
            String paramsValue = getProperties().get(Options.WRITE_TRANSFORM_PARAMS);
            if (paramsValue != null && paramsValue.trim().length() > 0) {
                addRestTransformParams(transform, paramsValue);
            }
            return Optional.of(transform);
        }
        return Optional.empty();
    }

    private void addRestTransformParams(ServerTransform transform, String paramsValue) {
        String delimiterValue = getProperties().get(Options.WRITE_TRANSFORM_PARAMS_DELIMITER);
        String delimiter = delimiterValue != null && delimiterValue.trim().length() > 0 ? delimiterValue : ",";
        String[] params = paramsValue.split(delimiter);
        if (params.length % 2 != 0) {
            throw new ConnectorException(
                String.format("The %s option must contain an equal number of parameter names and values; received: %s",
                    getOptionNameForMessage(Options.WRITE_TRANSFORM_PARAMS), paramsValue)
            );
        }
        for (int i = 0; i < params.length; i += 2) {
            transform.add(params[i], params[i + 1]);
        }
    }

    private void logBatchOnSuccess(WriteBatch batch) {
        int documentCount = batch.getItems().length;
        if (documentCount > 0) {
            WriteEvent firstEvent = batch.getItems()[0];
            // If the first event is the item added by DMSDK for the default metadata object, ignore it when showing
            // the count of documents in the batch.
            if (firstEvent.getTargetUri() == null && firstEvent.getMetadata() != null) {
                documentCount--;
            }
        }
        logBatchOnSuccess(documentCount, batch.getJobBatchNumber());
    }

    public void logBatchOnSuccess(int documentCount, long optionalJobBatchNumber) {
        WriteProgressLogger.logProgressIfNecessary(documentCount);
        if (Util.MAIN_LOGGER.isTraceEnabled() && optionalJobBatchNumber > 0) {
            Util.MAIN_LOGGER.trace("Wrote batch; length: {}; job batch number: {}", documentCount, optionalJobBatchNumber);
        }
    }

    boolean isUsingFileSchema() {
        return this.usingFileSchema;
    }

    int getFileSchemaContentPosition() {
        return fileSchemaContentPosition;
    }

    int getFileSchemaPathPosition() {
        return fileSchemaPathPosition;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }
}
