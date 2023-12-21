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

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class WriteContext extends ContextSupport {

    static final long serialVersionUID = 1;

    private final StructType schema;
    private final boolean usingFileSchema;

    private int fileSchemaContentPosition;
    private int fileSchemaPathPosition;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;

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

    WriteBatcher newWriteBatcher(DataMovementManager dataMovementManager) {
        WriteBatcher writeBatcher = dataMovementManager
            .newWriteBatcher()
            .withBatchSize((int) getNumericOption(Options.WRITE_BATCH_SIZE, 100, 1))
            .withThreadCount((int) getNumericOption(Options.WRITE_THREAD_COUNT, 4, 1));

        if (logger.isDebugEnabled()) {
            writeBatcher.onBatchSuccess(this::logBatchOnSuccess);
        }

        String temporalCollection = getProperties().get(Options.WRITE_TEMPORAL_COLLECTION);
        if (temporalCollection != null && temporalCollection.trim().length() > 0) {
            writeBatcher.withTemporalCollection(temporalCollection);
        }

        configureRestTransform(writeBatcher);

        return writeBatcher;
    }

    DocBuilder newDocBuilder() {
        DocBuilderFactory factory = new DocBuilderFactory()
            .withCollections(getProperties().get(Options.WRITE_COLLECTIONS))
            .withPermissions(getProperties().get(Options.WRITE_PERMISSIONS));

        String uriTemplate = getProperties().get(Options.WRITE_URI_TEMPLATE);
        if (uriTemplate != null && uriTemplate.trim().length() > 0) {
            factory.withUriMaker(new SparkRowUriMaker(uriTemplate));
            Stream.of(Options.WRITE_URI_PREFIX, Options.WRITE_URI_SUFFIX, Options.WRITE_URI_REPLACE).forEach(option -> {
                String value = getProperties().get(option);
                if (value != null && value.trim().length() > 0) {
                    logger.warn("Option {} will be ignored since option {} was specified.", option, Options.WRITE_URI_TEMPLATE);
                }
            });
        } else {
            String uriSuffix = null;
            if (hasOption(Options.WRITE_URI_SUFFIX)) {
                uriSuffix = getProperties().get(Options.WRITE_URI_SUFFIX);
            } else if (!isUsingFileSchema()) {
                uriSuffix = ".json";
            }
            factory.withUriMaker(new StandardUriMaker(
                getProperties().get(Options.WRITE_URI_PREFIX), uriSuffix,
                getProperties().get(Options.WRITE_URI_REPLACE)
            ));
        }

        return factory.newDocBuilder();
    }

    private void configureRestTransform(WriteBatcher writeBatcher) {
        String transformName = getProperties().get(Options.WRITE_TRANSFORM_NAME);
        if (transformName != null && transformName.trim().length() > 0) {
            ServerTransform transform = new ServerTransform(transformName);
            String paramsValue = getProperties().get(Options.WRITE_TRANSFORM_PARAMS);
            if (paramsValue != null && paramsValue.trim().length() > 0) {
                addRestTransformParams(transform, paramsValue);
            }
            writeBatcher.withTransform(transform);
        }
    }

    private void addRestTransformParams(ServerTransform transform, String paramsValue) {
        String delimiterValue = getProperties().get(Options.WRITE_TRANSFORM_PARAMS_DELIMITER);
        String delimiter = delimiterValue != null && delimiterValue.trim().length() > 0 ? delimiterValue : ",";
        String[] params = paramsValue.split(delimiter);
        if (params.length % 2 != 0) {
            throw new IllegalArgumentException(
                String.format("The %s option must contain an equal number of parameter names and values; received: %s",
                    Options.WRITE_TRANSFORM_PARAMS, paramsValue)
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
            // the count of documents in the batch.
            if (firstEvent.getTargetUri() == null && firstEvent.getMetadata() != null) {
                docCount--;
            }
        }
        logger.debug("Wrote batch; length: {}; job batch number: {}", docCount, batch.getJobBatchNumber());
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
}
