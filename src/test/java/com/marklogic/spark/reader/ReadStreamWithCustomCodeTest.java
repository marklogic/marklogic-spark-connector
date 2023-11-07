package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadStreamWithCustomCodeTest extends AbstractIntegrationTest {

    @Test
    void javascriptBatchIds(@TempDir Path tempDir) throws Exception {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_BATCH_IDS_JAVASCRIPT, "Sequence.from([1, 2, 3, 4, 5])");
    }

    @Test
    void xqueryBatchIds(@TempDir Path tempDir) throws Exception {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_BATCH_IDS_XQUERY, "(1, 2, 3, 4, 5)");
    }

    @Test
    void invokeBatchIds(@TempDir Path tempDir) throws Exception {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_BATCH_IDS_INVOKE, "/getBatchIds.sjs");
    }

    /**
     * Expects the given batch IDs approach to return batch IDs of 1, 2, 3, 4, and 5.
     * Those are then used to invoke a simple SJS reader 5 times, each time returning a single JSON documents. Then
     * the WriteBatcher writer is used to write those documents to MarkLogic.
     */
    private void verifyFiveDocumentsAreWritten(@TempDir Path tempDir, String batchIdsOption, String batchIdsValue) {
        DataStreamWriter writer = newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_NUM_PARTITIONS, 1)

            // Defines the approach for retrieving batch IDs.
            .option(batchIdsOption, batchIdsValue)

            // Will be invoked once per batch ID.
            .option(Options.READ_JAVASCRIPT, "var BATCH_ID; " +
                "const row = {\"batchId\": BATCH_ID, \"hello\": \"world\"}; Sequence.from([row])")

            // Need a custom schema for the simple JSON documents returned by the reader.
            .schema(new StructType()
                .add("batchId", DataTypes.StringType)
                .add("hello", DataTypes.StringType))
            .load()
            
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "read-stream")
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
            .option(Options.WRITE_URI_TEMPLATE, "/read-stream/{batchId}.json")
            .option(Options.WRITE_URI_PREFIX, "/")

            // Required option by Spark when streaming.
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath());

        try {
            writer.start().processAllAvailable();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        assertCollectionSize("read-stream", 5);

        for (int i = 1; i <= 5; i++) {
            JsonNode doc = readJsonDocument("/read-stream/" + i + ".json");
            assertEquals(i, Integer.parseInt(doc.get("batchId").asText()));
            assertEquals("world", doc.get("hello").asText());
        }
    }
}
