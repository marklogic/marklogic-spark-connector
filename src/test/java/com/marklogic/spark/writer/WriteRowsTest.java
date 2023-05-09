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

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class WriteRowsTest extends AbstractIntegrationTest {

    private final static String EXPECTED_COLLECTION = "my-test-data";

    @Test
    void streaming() throws Exception {
        StreamingQuery stream = newSparkSession().readStream()
            .schema(new StructType()
                .add("docNum", DataTypes.IntegerType)
                .add("docName", DataTypes.StringType))
            .option("header", true)
            .format("csv")
            // Spark complains if this is a single file path - ??
            .csv("src/test/resources/data*.csv")
            .writeStream()
            .format("com.marklogic.spark")
            .option(Options.WRITE_BATCH_SIZE, 10)
            .option(Options.WRITE_THREAD_COUNT, 1)
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", "spark-test-user")
            .option("spark.marklogic.client.password", "spark")
            .option("spark.marklogic.client.authType", "digest")

            // This should really be a new temp directory
            .option("checkpointLocation", "/Users/rrudin/spark-temp/")
            .start();

//        stream.awaitTermination(5000);
        stream.processAllAvailable();
        System.out.println("All done!");

        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void defaultBatchSizeAndThreadCount() {
        newWriterFromCsvFile().save();
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void batchSizeGreaterThanNumberOfRowsToWrite() {
        newWriterFromCsvFile()
            .option(Options.WRITE_BATCH_SIZE, 1000)
            .save();

        // Verifies that the docs were written during the "commit()" call, as the WriteBatcher is expected to be
        // flushed during that call.
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void twoPartitions() {
        newWriterFromCsvFile(2).save();

        // Just verifies that the operation succeeds with multiple partitions. Check the logging to see that two
        // partitions were in fact created, each with its own WriteBatcher.
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void invalidThreadCount() {
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriterFromCsvFile().option(Options.WRITE_THREAD_COUNT, 0).save()
        );

        assertTrue(ex.getCause() instanceof IllegalArgumentException, "Unexpected cause: " + ex.getCause().getClass());
        assertEquals("Value of 'spark.marklogic.write.threadCount' option must be 1 or greater", ex.getCause().getMessage());
        verifyNoDocsWereWritten();
    }

    @Test
    void invalidBatchSize() {
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriterFromCsvFile().option(Options.WRITE_BATCH_SIZE, 0).save()
        );

        assertTrue(ex.getCause() instanceof IllegalArgumentException, "Unexpected cause: " + ex.getCause().getClass());
        assertEquals("Value of 'spark.marklogic.write.batchSize' option must be 1 or greater", ex.getCause().getMessage(),
            "Note that batchSize is very different for writing than it is for reading. For writing, it specifies the " +
                "exact number of documents to send to MarkLogic in each call. For reading, it used to determine how " +
                "many requests will be made by a partition, and zero is a valid value for reading.");
        verifyNoDocsWereWritten();
    }

    /**
     * Verifies that the commit() method in the data writer will fail when it flushes and waits. This is done by
     * using a batch size greater than the number of documents to be written (200) so that nothing is written until the
     * commit() method is invoked.
     */
    @Test
    void userNotPermittedToWriteAndFailOnCommit() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriterFromCsvFile()
                .option("spark.marklogic.client.username", "spark-no-write-user")
                .option(Options.WRITE_BATCH_SIZE, 500)
                .save()
        );

        verifyFailureIsDueToLackOfPermission(ex);
    }

    /**
     * Uses a batch size of 1 to ensure that the write() method in the data writer should fail, as each write() call
     * should result in a request to MarkLogic, which should cause a failure before commit() is called.
     */
    @Test
    void userNotPermittedToWriteAndFailOnWrite() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriterFromCsvFile()
                .option("spark.marklogic.client.username", "spark-no-write-user")
                .option(Options.WRITE_BATCH_SIZE, 1)
                .option(Options.WRITE_THREAD_COUNT, 1)
                .save()
        );

        verifyFailureIsDueToLackOfPermission(ex);
    }

    private void verifyFailureIsDueToLackOfPermission(SparkException ex) {
        assertTrue(ex.getCause() instanceof IOException, "Unexpected cause: " + ex.getCause().getClass());
        assertTrue(ex.getCause().getMessage().contains("Server Message: You do not have permission to this method and URL"),
            "Unexpected cause message: " + ex.getCause().getMessage());
        verifyNoDocsWereWritten();
    }

    private DataFrameWriter newWriterFromCsvFile() {
        return newWriterFromCsvFile(1);
    }

    private DataFrameWriter newWriterFromCsvFile(int partitionCount) {
        return newSparkSession().read()
            .option("header", true)
            .format("csv")
            .csv("src/test/resources/data.csv")
            .repartition(partitionCount)
            .write()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", "spark-test-user")
            .option("spark.marklogic.client.password", "spark")
            .option("spark.marklogic.client.authType", "digest")
            .mode(SaveMode.Append);
    }

    private void verifyTwoHundredDocsWereWritten() {
        final int expectedCollectionSize = 200;
        String uri = getUrisInCollection(EXPECTED_COLLECTION, expectedCollectionSize).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("docNum"));
        assertTrue(doc.has("docName"));
    }

    private void verifyNoDocsWereWritten() {
        assertCollectionSize(EXPECTED_COLLECTION, 0);
    }
}
