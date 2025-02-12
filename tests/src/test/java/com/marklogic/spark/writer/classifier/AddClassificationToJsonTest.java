/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;

class AddClassificationToJsonTest extends AbstractIntegrationTest {

    private static final String API_KEY = System.getenv("SEMAPHORE_API_KEY");

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void chunkAndAddClassificationToJsonInOriginalJsonDoc() {
        readAndStartWrite()
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));
        assertTrue(doc.get("chunks").get(0).get("classification").has("URL"));
    }

    /**
     * Tests the use case where a user wants to split the text into chunks and classify each chunk, all
     * as part of one write process.
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void sidecarChunksAddClassificationToJson() {
        readAndStartWrite()
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));

        doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.get("chunks").get(0).get("classification").has("URL"));
        assertTrue(doc.get("chunks").get(1).get("classification").has("URL"));
    }

    /**
     * Verifies that when a semaphore server is not specified, classification is not added to chunks.
     */
    @Test
    void noClassificationAddedToJsonWhenNoSemaphoreServerSpecified() {
        readAndStartWrite()
            .option(Options.WRITE_CLASSIFIER_HOST, "")
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertFalse(doc.get("chunks").get(0).has("classification"));
        assertFalse(doc.get("chunks").get(1).has("classification"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void classifyJsonContentsWithoutChunking() {
        readAndStartWrite()
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));
        assertFalse(doc.has("chunks"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void chunkAndAddClassificationOnlyToChunksInOriginalDoc() {
        readAndStartWrite()
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on max chunk size of 500.");
    }

    @Test
    @Disabled("Placeholder for a future test for data that is not UTF-8")
    void classifyNonUtf8JsonData() {
        assertTrue(true);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private DataFrameWriter readAndStartWrite() {
        return readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")

            // Use minimal options for this test to verify that default protocol/port/tokenEndpoint are used.
            .option(Options.WRITE_CLASSIFIER_APIKEY, API_KEY)
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_PATH, "/cls/dev/cs1/");
    }
}
