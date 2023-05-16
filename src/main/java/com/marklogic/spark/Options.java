package com.marklogic.spark;

/**
 * TODO Will merge ReadConstants into this.
 */
public interface Options {

    String CLIENT_URI = "spark.marklogic.client.uri";

    String READ_OPTIC_DSL = "spark.marklogic.read.opticDsl";
    String READ_NUM_PARTITIONS = "spark.marklogic.read.numPartitions";
    String READ_BATCH_SIZE = "spark.marklogic.read.batchSize";

    String WRITE_BATCH_SIZE = "spark.marklogic.write.batchSize";
    String WRITE_THREAD_COUNT = "spark.marklogic.write.threadCount";

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
