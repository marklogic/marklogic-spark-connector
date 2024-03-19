package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

class ReadZipOfJsonFilesTest extends AbstractIntegrationTest {

    /**
     * Note that we'll still have issues with JSON lines files in zips. This only works
     * because we're using our own zip reader.
     */
    @Test
    void test() {
        Dataset<Row> reader = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/json-objects.zip");

        defaultWrite(reader.write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_REPLACE, ".*/zip-files,''")
            .option(Options.WRITE_URI_TEMPLATE, "/zip/{number}.json")
            .option(Options.WRITE_COLLECTIONS, "zip-test")
        );
    }

    // With template - 148s
    // Without template - 80s
    @Test
    void perf() {
        long start = System.currentTimeMillis();
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("100k.zip")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, "admin:admin@localhost:8116")
            .option(Options.WRITE_URI_REPLACE, ".*/zip-files,''")
//            .option(Options.WRITE_URI_TEMPLATE, "/zip/{address_id}.json")
            .option(Options.WRITE_COLLECTIONS, "zip-test")
            .mode(SaveMode.Append)
            .save();
        System.out.println("TIME: " + (System.currentTimeMillis() - start));
    }
}
