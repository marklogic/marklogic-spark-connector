package com.marklogic.spark.reader.customcode;

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

class ReadStreamWithCustomCodeTest extends AbstractIntegrationTest {

    @Test
    void javascriptBatchIds(@TempDir Path tempDir) {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_PARTITIONS_JAVASCRIPT, "Sequence.from([1, 2, 3, 4, 5])");
    }

    @Test
    void xqueryBatchIds(@TempDir Path tempDir) {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_PARTITIONS_XQUERY, "(1, 2, 3, 4, 5)");
    }

    @Test
    void invokeBatchIds(@TempDir Path tempDir) {
        verifyFiveDocumentsAreWritten(tempDir, Options.READ_PARTITIONS_INVOKE, "/getBatchIds.sjs");
    }

    /**
     * Expects the given batch IDs approach to return batch IDs of 1, 2, 3, 4, and 5.
     * Those are then used to invoke a simple SJS reader 5 times, each time returning a single JSON documents. Then
     * the WriteBatcher writer is used to write those documents to MarkLogic.
     */
    private void verifyFiveDocumentsAreWritten(@TempDir Path tempDir, String partitionsOption, String partitionsValue) {
        DataStreamWriter writer = newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())

            // Defines the approach for retrieving partitions.
            .option(partitionsOption, partitionsValue)

            // Will be invoked once per partition.
            .option(Options.READ_JAVASCRIPT,
                "const row = {\"partition\": PARTITION, \"hello\": \"world\"}; Sequence.from([row])")

            // Need a custom schema for the simple JSON documents returned by the reader.
            .schema(new StructType()
                .add("partition", DataTypes.StringType)
                .add("hello", DataTypes.StringType))

            // Example of a user-defined var that isn't used; this is fine, just verifying it doesn't throw an error.
            .option(Options.READ_VARS_PREFIX + "UNUSED_VAR", "this shouldn't throw an error")

            .load()

            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "read-stream")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/read-stream/{partition}.json")
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
            assertEquals(i, Integer.parseInt(doc.get("partition").asText()));
            assertEquals("world", doc.get("hello").asText());
        }
    }

    /**
     * Demonstrates that user-defined variables for reading will be sent both to the batch IDs script and to the
     * read script.
     */
    @Test
    void javascriptBatchIdsWithUserDefinedVariables(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_PARTITIONS_JAVASCRIPT, "var USER_VAR_EXAMPLE; Sequence.from([1, USER_VAR_EXAMPLE])")
            .option(Options.READ_JAVASCRIPT,
                "const row = {\"partition\": PARTITION, \"var\": USER_VAR_EXAMPLE}; " +
                "Sequence.from([row])")
            .option(Options.READ_VARS_PREFIX + "USER_VAR_EXAMPLE", "2")
            .schema(new StructType()
                .add("partition", DataTypes.StringType)
                .add("var", DataTypes.StringType))
            .load()
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "read-stream")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/read-stream/{partition}.json")
            .option(Options.WRITE_URI_PREFIX, "/")
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("read-stream", 2);

        for (int i = 1; i <= 2; i++) {
            JsonNode doc = readJsonDocument("/read-stream/" + i + ".json");
            assertEquals(i, Integer.parseInt(doc.get("partition").asText()));
            assertEquals("2", doc.get("var").asText());
        }
    }

    @Test
    void singlePartition(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_PARTITIONS_JAVASCRIPT, "Sequence.from([1])")
            .option(Options.READ_JAVASCRIPT, "Sequence.from([PARTITION])")
            .load()
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "single-partition-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("single-partition-test", 1);
    }
}
