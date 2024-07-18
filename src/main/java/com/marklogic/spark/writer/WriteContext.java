/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.TripleRowSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class WriteContext extends ContextSupport {

    static final long serialVersionUID = 1;
    private static final AtomicInteger progressTracker = new AtomicInteger();

    private final StructType schema;
    private final boolean usingFileSchema;
    private final int batchSize;
    private final int logProgress;

    private int fileSchemaContentPosition;
    private int fileSchemaPathPosition;

    // This unfortunately is not final as we don't know it when this object is created.
    private int numPartitions;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;
        this.batchSize = (int) getNumericOption(Options.WRITE_BATCH_SIZE, 100, 1);
        this.logProgress = (int) getNumericOption(Options.WRITE_LOG_PROGRESS, 10000, 0);

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

    int getThreadCount() {
        return (int) getNumericOption(Options.WRITE_THREAD_COUNT, 4, 1);
    }

    int getTotalThreadCount() {
        return (int) getNumericOption(Options.WRITE_TOTAL_THREAD_COUNT, 0, 1);
    }

    int getThreadCountPerPartition() {
        int totalThreadCount = getTotalThreadCount();
        if (totalThreadCount > 0 && this.numPartitions > 0) {
            return (int) Math.ceil((double) totalThreadCount / (double) numPartitions);
        }
        return 0;
    }

    WriteBatcher newWriteBatcher(DataMovementManager dataMovementManager) {
        final int threadCount = getTotalThreadCount() > 0 ?
            getThreadCountPerPartition() : getThreadCount();

        Util.MAIN_LOGGER.info("Creating new batcher with thread count of {} and batch size of {}.", threadCount, batchSize);
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
        } else if (!isUsingFileSchema() && !DocumentRowSchema.SCHEMA.equals(this.schema) && !TripleRowSchema.SCHEMA.equals(this.schema)) {
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
        int docCount = batch.getItems().length;
        if (docCount > 0) {
            WriteEvent firstEvent = batch.getItems()[0];
            // If the first event is the item added by DMSDK for the default metadata object, ignore it when showing
            // the count of documents in the batch.
            if (firstEvent.getTargetUri() == null && firstEvent.getMetadata() != null) {
                docCount--;
            }
        }
        if (this.logProgress > 0) {
            logProgressIfNecessary(docCount);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Wrote batch; length: {}; job batch number: {}", docCount, batch.getJobBatchNumber());
        }
    }

    private void logProgressIfNecessary(int docCount) {
        int sum = progressTracker.addAndGet(docCount);
        if (sum >= logProgress) {
            int lowerBound = sum / (this.logProgress);
            int upperBound = (lowerBound * this.logProgress) + this.batchSize;
            if (sum >= lowerBound && sum < upperBound) {
                Util.MAIN_LOGGER.info("Documents written: {}", sum);
            }
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
