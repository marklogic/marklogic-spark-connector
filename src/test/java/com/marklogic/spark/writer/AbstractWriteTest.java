package com.marklogic.spark.writer;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

abstract class AbstractWriteTest extends AbstractIntegrationTest {

    protected final static String COLLECTION = "write-test";

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
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
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
            .format("com.marklogic.spark")
            .mode(SaveMode.Append)
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", "spark-test-user")
            .option("spark.marklogic.client.password", "spark")
            .option("spark.marklogic.client.authType", "digest");
    }
}
