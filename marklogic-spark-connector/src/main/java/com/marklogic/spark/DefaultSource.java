/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.document.DocumentTable;
import com.marklogic.spark.reader.file.TripleRowSchema;
import com.marklogic.spark.reader.optic.OpticReadContext;
import com.marklogic.spark.reader.optic.SchemaInferrer;
import com.marklogic.spark.writer.WriteContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The name "DefaultSource" is used here so that this connector can be loaded using the Spark V2 approach, where the
 * user specifies a package name and the class name is assumed to be "DefaultSource".
 */
public class DefaultSource implements TableProvider, DataSourceRegister {

    private static final Logger logger = LoggerFactory.getLogger("com.marklogic.spark");

    @Override
    public String shortName() {
        // Allows for "marklogic" to be used instead of "com.marklogic.spark".
        return "marklogic";
    }

    /**
     * If no schema is provided when reading data, Spark invokes this before getTable is invoked.
     * <p>
     * This will not be invoked during a write operation, as the schema in the Dataset being written will be used
     * instead.
     */
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        final Map<String, String> properties = options.asCaseSensitiveMap();
        if (isFileOperation(properties)) {
            final String type = properties.get(Options.READ_FILES_TYPE);
            return "rdf".equalsIgnoreCase(type) ? TripleRowSchema.SCHEMA : DocumentRowSchema.SCHEMA;
        }
        if (isReadDocumentsOperation(properties)) {
            return DocumentRowSchema.SCHEMA;
        } else if (isReadTriplesOperation(properties)) {
            return TripleRowSchema.SCHEMA;
        } else if (Util.isReadWithCustomCodeOperation(properties)) {
            return new StructType().add("URI", DataTypes.StringType);
        }
        return inferSchemaFromOpticQuery(properties);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        if (isFileOperation(properties)) {
            // Not yet supporting progress logging for file operations.
            return new MarkLogicFileTable(SparkSession.active(),
                new CaseInsensitiveStringMap(properties),
                JavaConverters.asScalaBuffer(getPaths(properties)), schema
            );
        }

        final ContextSupport tempContext = new ContextSupport(properties);

        // The appropriate progress logger is reset here so that when the connector is used repeatedly in an
        // environment like PySpark, the counts start with zero on each new Spark job.
        final long readProgressInterval = tempContext.getNumericOption(Options.READ_LOG_PROGRESS, 0, 0);
        if (isReadDocumentsOperation(properties)) {
            ReadProgressLogger.initialize(readProgressInterval, "Documents read: {}");
            return new DocumentTable(DocumentRowSchema.SCHEMA);
        } else if (isReadTriplesOperation(properties)) {
            ReadProgressLogger.initialize(readProgressInterval, "Triples read: {}");
            return new DocumentTable(TripleRowSchema.SCHEMA);
        } else if (properties.get(Options.READ_OPTIC_QUERY) != null) {
            final long batchSize = tempContext.getNumericOption(Options.READ_BATCH_SIZE, OpticReadContext.DEFAULT_BATCH_SIZE, 0);
            if (readProgressInterval > 0 && batchSize > readProgressInterval) {
                Util.MAIN_LOGGER.info("Batch size is greater than interval for reading progress, so will use batch size of {} to log progress instead.",
                    batchSize);
                ReadProgressLogger.initialize(batchSize, "Rows read: {}");
            } else {
                ReadProgressLogger.initialize(readProgressInterval, "Rows read: {}");
            }
            return new MarkLogicTable(schema, properties);
        } else if (Util.isReadWithCustomCodeOperation(properties)) {
            ReadProgressLogger.initialize(readProgressInterval, "Items read: {}");
            return new MarkLogicTable(schema, properties);
        }

        final long writeProgressInterval = tempContext.getNumericOption(Options.WRITE_LOG_PROGRESS, 0, 0);
        String message = Util.isWriteWithCustomCodeOperation(properties) ? "Items processed: {}" : "Documents written: {}";
        WriteProgressLogger.initialize(writeProgressInterval, message);
        return new MarkLogicTable(new WriteContext(schema, properties));
    }

    /**
     * Per https://spark.apache.org/docs/3.2.4/api/java/org/apache/spark/sql/connector/catalog/TableProvider.html#supportsExternalMetadata-- ,
     * this returns true as we allow for a user to provide their own schema instead of requiring schema inference.
     *
     * @return
     */
    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    private boolean isFileOperation(Map<String, String> properties) {
        return properties.containsKey("path") || properties.containsKey("paths");
    }

    private boolean isReadDocumentsOperation(Map<String, String> properties) {
        return properties.containsKey(Options.READ_DOCUMENTS_QUERY) ||
            properties.containsKey(Options.READ_DOCUMENTS_STRING_QUERY) ||
            properties.containsKey(Options.READ_DOCUMENTS_COLLECTIONS) ||
            properties.containsKey(Options.READ_DOCUMENTS_DIRECTORY) ||
            properties.containsKey(Options.READ_DOCUMENTS_OPTIONS) ||
            properties.containsKey(Options.READ_DOCUMENTS_URIS);
    }

    private boolean isReadTriplesOperation(Map<String, String> properties) {
        return Util.hasOption(properties,
            Options.READ_TRIPLES_GRAPHS,
            Options.READ_TRIPLES_COLLECTIONS,
            Options.READ_TRIPLES_QUERY,
            Options.READ_TRIPLES_STRING_QUERY,
            Options.READ_TRIPLES_URIS,
            Options.READ_TRIPLES_DIRECTORY
        );
    }

    private StructType inferSchemaFromOpticQuery(Map<String, String> caseSensitiveOptions) {
        final String query = caseSensitiveOptions.get(Options.READ_OPTIC_QUERY);
        if (query == null || query.trim().isEmpty()) {
            throw new ConnectorException(Util.getOptionNameForErrorMessage("spark.marklogic.read.noOpticQuery"));
        }
        RowManager rowManager = new ContextSupport(caseSensitiveOptions).connectToMarkLogic().newRowManager();
        RawQueryDSLPlan dslPlan = rowManager.newRawQueryDSLPlan(new StringHandle(query));
        try {
            // columnInfo is what forces a minimum MarkLogic version of 10.0-9 or higher.
            StringHandle columnInfoHandle = rowManager.columnInfo(dslPlan, new StringHandle());
            StructType schema = SchemaInferrer.inferSchema(columnInfoHandle.get());
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                logger.debug("Inferred schema from Optic columnInfo: {}", schema);
            }

            // Hacking in columns for params.
            List<String> paramNames = caseSensitiveOptions.keySet().stream()
                .filter(key -> key.startsWith(Options.READ_OPTIC_PARAM_PREFIX))
                .map(key -> key.substring(Options.READ_OPTIC_PARAM_PREFIX.length()))
                .collect(Collectors.toList());
            /**
             * Here's our main problem - we need to add the op.param to the Spark schema so that a user can then
             * filter on it without error. To do that, we need to know the Spark type to use. But we have no way of
             * knowing how the op.param is going to be used unless we were to parse the query somehow to try to
             * determine what column type is associated with the op.param.
             *
             * Alternatively, we could require that the user specify the type of the op.param in their Spark options,
             * with string being the default. Example - spark.marklogic.read.opticParam.myValue.type=string. That
             * would allow us to build the correct schema here.
             */
            for (String paramName : paramNames) {
                schema = schema.add(paramName, DataTypes.StringType, true);
            }
            Util.MAIN_LOGGER.warn("SCHEMA: " + schema.prettyJson());

            return schema;
        } catch (Exception ex) {
            throw new ConnectorException(String.format("Unable to run Optic query %s; cause: %s", query, ex.getMessage()), ex);
        }
    }

    private List<String> getPaths(Map<String, String> properties) {
        return properties.containsKey("path") ?
            Arrays.asList(properties.get("path")) :
            Util.parsePaths(properties.get("paths"));
    }
}
