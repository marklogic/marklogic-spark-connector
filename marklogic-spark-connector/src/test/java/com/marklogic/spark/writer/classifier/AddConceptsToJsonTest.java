/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;

class AddConceptsToJsonTest extends AbstractIntegrationTest {

    /**
     * Tests the use case where a user wants to split the text into chunks and classify each chunk, all
     * as part of one write process.
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void splitToSeparateDocumentsAndAddConcepts() {
        final String apiKey = System.getenv("SEMAPHORE_API_KEY");
        assertNotNull(apiKey);

        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_HTTPS, true)
            .option(Options.WRITE_CLASSIFIER_PORT, "443")
            .option(Options.WRITE_CLASSIFIER_ENDPOINT, "/cls/dev/cs1/")
            .option(Options.WRITE_CLASSIFIER_APIKEY, apiKey)
            .option(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.get("chunks").get(0).has("concepts"));
        assertTrue(doc.get("chunks").get(1).has("concepts"));
    }

    /**
     * Verifies that when a semaphore server is not specified, concepts are not added to chunks.
     */
    @Test
    void noConceptsAddedWhenNoSemaphoreServerSpecified() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertFalse(doc.get("chunks").get(0).has("concepts"));
        assertFalse(doc.get("chunks").get(1).has("concepts"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void specifyAlternateNameForConceptsArray() {
        final String apiKey = System.getenv("SEMAPHORE_API_KEY");
        assertNotNull(apiKey);

        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_HTTPS, true)
            .option(Options.WRITE_CLASSIFIER_PORT, "443")
            .option(Options.WRITE_CLASSIFIER_ENDPOINT, "/cls/dev/cs1/")
            .option(Options.WRITE_CLASSIFIER_APIKEY, apiKey)
            .option(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/")
            .option(Options.WRITE_CLASSIFIER_CONCEPTS_ARRAY, "differentName")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.get("chunks").get(0).has("differentName"));
        assertTrue(doc.get("chunks").get(1).has("differentName"));
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
