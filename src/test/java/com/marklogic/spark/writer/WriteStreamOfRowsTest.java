/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteStreamOfRowsTest extends AbstractWriteTest {

    private static final StructType SCHEMA = new StructType()
        .add("Name", DataTypes.StringType)
        .add("House", DataTypes.StringType);

    @Test
    void streamRowsFromCsvFile(@TempDir Path tempDir) throws TimeoutException {
        final String collection = "hogwarts";

        newDefaultStreamWriter(tempDir)
            .option(Options.WRITE_URI_TEMPLATE, "/hogwarts/{Name}.json")
            .option(Options.WRITE_COLLECTIONS, collection)
            .start()
            .processAllAvailable();

        assertCollectionSize(collection, 9);
        JsonNode doc = readJsonDocument("/hogwarts/Hermione Granger.json");
        assertEquals("Gryffindor", doc.get("House").asText());
        assertEquals("Hermione Granger", doc.get("Name").asText());
    }

    @Test
    void invalidTransform(@TempDir Path tempDir) throws Exception {
        StreamingQuery query = newDefaultStreamWriter(tempDir)
            .option(Options.WRITE_TRANSFORM_NAME, "this-doesnt-exist")
            .start();

        // No error will be thrown yet as the streaming occurs in the background, though we'll likely see errors
        // from the background thread. Calling processAllAvailable should force an error to occur.

        StreamingQueryException ex = assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
        Throwable cause = getCauseFromWriterException(ex);
        cause = isSpark340OrHigher() ? cause : cause.getCause();
        assertTrue(cause.getMessage().contains("Extension this-doesnt-exist or a dependency does not exist"),
            "Unexpected error: " + cause);
    }

    private DataStreamWriter newDefaultStreamWriter(Path tempDir) {
        return newSparkSession().readStream()
            .schema(SCHEMA)
            .option("header", true)
            .format("csv")
            .load("src/test/resources/inputForStream")
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS);
    }

}
