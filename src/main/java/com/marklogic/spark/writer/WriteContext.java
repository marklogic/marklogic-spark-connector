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

import java.util.Map;
import java.util.stream.Stream;

public class WriteContext extends ContextSupport {

    final static long serialVersionUID = 1;

    private final StructType schema;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;
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
            Stream.of(Options.WRITE_URI_PREFIX, Options.WRITE_URI_SUFFIX).forEach(option -> {
                if (getProperties().containsKey(option)) {
                    logger.warn("Option {} will be ignored since option {} was specified.", option, Options.WRITE_URI_TEMPLATE);
                }
            });
        } else {
            final String uriSuffix = getProperties().containsKey(Options.WRITE_URI_SUFFIX) ?
                getProperties().get(Options.WRITE_URI_SUFFIX) :
                ".json";
            factory.withSimpleUriStrategy(getProperties().get(Options.WRITE_URI_PREFIX), uriSuffix);
        }

        return factory.newDocBuilder();
    }

    boolean isAbortOnFailure() {
        return !"false".equalsIgnoreCase(getProperties().get(Options.WRITE_ABORT_ON_FAILURE));
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
}
