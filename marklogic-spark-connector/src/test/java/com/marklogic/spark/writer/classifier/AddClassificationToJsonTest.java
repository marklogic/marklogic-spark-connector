/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.classifier.MockSemaphoreProxy;
import nl.altindag.log.LogCaptor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AddClassificationToJsonTest extends AbstractIntegrationTest {

    @Test
    void chunkAndAddClassificationToJsonInOriginalJsonDoc() {
        readAndStartWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(3))
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 0)
            .mode(SaveMode.Append)
            .save();

        assertTrue(MockSemaphoreProxy.isClosed());

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("STRUCTUREDDOCUMENT"));
        assertTrue(doc.get("chunks").get(0).get("classification").has("SYSTEM"));
    }

    /**
     * Tests the use case where a user wants to split the text into chunks and classify each chunk, all
     * as part of one write process.
     */
    @Test
    void sidecarChunksAddClassificationToJson() {
        readAndStartWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(3))
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        assertTrue(MockSemaphoreProxy.isClosed());

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("STRUCTUREDDOCUMENT"));

        doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.get("chunks").get(0).get("classification").has("SYSTEM"));
        assertTrue(doc.get("chunks").get(1).get("classification").has("SYSTEM"));
    }

    /**
     * Verifies that when a semaphore server is not specified, classification is not added to chunks.
     */
    @Test
    void noClassificationAddedToJsonWhenNoSemaphoreServerSpecified() {
        readAndStartWrite()
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
    void classifyJsonContentsWithoutChunking() {
        readAndStartWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(1))
            .mode(SaveMode.Append)
            .save();

        assertTrue(MockSemaphoreProxy.isClosed());

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("STRUCTUREDDOCUMENT"));
        assertFalse(doc.has("chunks"));
    }

    @Test
    void chunkAndAddClassificationOnlyToChunksInOriginalDoc() {
        readAndStartWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(5))
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 0)
            .mode(SaveMode.Append)
            .save();

        assertTrue(MockSemaphoreProxy.isClosed());
        assertEquals(1, MockSemaphoreProxy.getDocumentClassificationCount(),
            "By default, the whole document should be classified in its own request");

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("STRUCTUREDDOCUMENT"));
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on max chunk size of 500.");

        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        for (int i = 0; i < chunks.size(); i++) {
            JsonNode chunk = chunks.get(i);
            assertTrue(chunk.has("text"));
            assertTrue(chunk.has("classification"));
            assertTrue(chunk.get("classification").has("SYSTEM"));
        }
    }

    @Test
    void skipDocumentWithChunksInOriginalJsonDoc() {
        readAndStartWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(4))
            .option(Options.WRITE_CLASSIFIER_SKIP_DOCUMENT, true)
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 0)
            .mode(SaveMode.Append)
            .save();

        assertTrue(MockSemaphoreProxy.isClosed());
        assertEquals(0, MockSemaphoreProxy.getDocumentClassificationCount(),
            "No whole-document classification request should be sent when skipDocument is true");

        JsonNode doc = readJsonDocument("/split-test.json");
        assertFalse(doc.has("classification"),
            "Source document envelope should not have a document-level classification element when skipDocument is true");
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on max chunk size of 500.");

        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        for (int i = 0; i < chunks.size(); i++) {
            JsonNode chunk = chunks.get(i);
            assertTrue(chunk.has("text"));
            assertTrue(chunk.has("classification"));
            assertTrue(chunk.get("classification").has("SYSTEM"));
        }
    }

    @Test
    void skipDocumentWithoutSplitterLogsWarning() {
        try (LogCaptor logCaptor = LogCaptor.forName(Util.MAIN_LOGGER.getName())) {
            readAndStartWrite()
                .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(1))
                .option(Options.WRITE_CLASSIFIER_SKIP_DOCUMENT, true)
                .mode(SaveMode.Append)
                .save();

            assertTrue(logCaptor.getWarnLogs().contains(
                "Whole-document classification is skipped and no text splitter is configured; no text will be classified."),
                "Unexpected warn logs: " + logCaptor.getWarnLogs());
        }

        assertEquals(0, MockSemaphoreProxy.getDocumentClassificationCount());
        JsonNode doc = readJsonDocument("/split-test.json");
        assertFalse(doc.has("classification"));
        assertFalse(doc.has("chunks"));
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private DataFrameWriter readAndStartWrite() {
        // A single partition ensures one writer, so MockSemaphoreProxy's static state reflects the whole write.
        return readDocument("/marklogic-docs/java-client-intro.json")
            .coalesce(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json");
    }
}
