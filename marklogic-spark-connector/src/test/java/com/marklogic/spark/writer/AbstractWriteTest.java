/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

public abstract class AbstractWriteTest extends AbstractIntegrationTest {

    protected static final String COLLECTION = "write-test";

    protected DataFrameWriter newWriter() {
        return newWriter(1);
    }

    protected DataFrameWriter newWriterForSingleRow() {
        return newWriterWithDefaultConfig("temporal-data.csv", 1);
    }

    protected DataFrameWriter newWriter(int partitionCount) {
        return newWriterWithDefaultConfig("data.csv", partitionCount);
    }

    protected DataFrameWriter newWriterWithDefaultConfig(String csvFilename, int partitionCount) {
        return newWriterWithoutDocumentConfig(csvFilename, partitionCount)
            .option(Options.WRITE_COLLECTIONS, COLLECTION)
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_PREFIX, "/test/")
            .option(Options.WRITE_URI_SUFFIX, ".json");
    }

    protected DataFrameWriter newWriterWithoutDocumentConfig(String csvFilename, int partitionCount) {
        return newSparkSession().read()
            .option("header", true)
            .format("csv")
            .csv("src/test/resources/" + csvFilename)
            .repartition(partitionCount)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .option(Options.CLIENT_HOST, testConfig.getHost())
            .option(Options.CLIENT_PORT, testConfig.getRestPort())
            .option(Options.CLIENT_USERNAME, "spark-test-user")
            .option(Options.CLIENT_PASSWORD, "spark");
    }

    /**
     * In Spark 3.3, an error from our writer - such as an IllegalArgumentException for an invalid option - is two
     * layers deep in a SparkException. In Spark 3.4, it's one layer deep. This method is used to hide that from
     * test classes so that when we upgrade from Spark 3.3 to Spark 3.4, it's easy to update the tests.
     *
     * @param ex
     * @return
     */
    protected final Throwable getCauseFromWriterException(Exception ex) {
        return isSpark340OrHigher() ? ex.getCause() : ex.getCause().getCause();
    }
}
