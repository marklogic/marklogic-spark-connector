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
package com.marklogic.spark;

public interface Options {

    String CLIENT_URI = "spark.marklogic.client.uri";
    String CLIENT_SSL_ENABLED = "spark.marklogic.client.sslEnabled";

    String READ_OPTIC_QUERY = "spark.marklogic.read.opticQuery";
    String READ_NUM_PARTITIONS = "spark.marklogic.read.numPartitions";
    String READ_BATCH_SIZE = "spark.marklogic.read.batchSize";
    String READ_PUSH_DOWN_AGGREGATES = "spark.marklogic.read.pushDownAggregates";

    String WRITE_BATCH_SIZE = "spark.marklogic.write.batchSize";
    String WRITE_THREAD_COUNT = "spark.marklogic.write.threadCount";
    String WRITE_ABORT_ON_FAILURE = "spark.marklogic.write.abortOnFailure";

    String WRITE_COLLECTIONS = "spark.marklogic.write.collections";
    String WRITE_PERMISSIONS = "spark.marklogic.write.permissions";
    String WRITE_TEMPORAL_COLLECTION = "spark.marklogic.write.temporalCollection";
    String WRITE_URI_PREFIX = "spark.marklogic.write.uriPrefix";
    String WRITE_URI_SUFFIX = "spark.marklogic.write.uriSuffix";
    String WRITE_URI_TEMPLATE = "spark.marklogic.write.uriTemplate";

    String WRITE_TRANSFORM_NAME = "spark.marklogic.write.transform";
    String WRITE_TRANSFORM_PARAMS = "spark.marklogic.write.transformParams";
    String WRITE_TRANSFORM_PARAMS_DELIMITER = "spark.marklogic.write.transformParamsDelimiter";
}
