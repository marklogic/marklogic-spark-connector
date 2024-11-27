/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that when split text from text documents and then adding embeddings to the sidecar docs, the user doesn't
 * need to specify the location of the chunks. The connector is expected to determine the location based on whether the
 * sidecar docs are JSON or XML.
 */
class AddEmbeddingsFromTextTest extends AbstractIntegrationTest {

    private static final String TEST_EMBEDDING_FUNCTION_CLASS = "com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction";

    @Test
    void jsonSidecarDocuments() {
        prepareToWriteChunks()
            .mode(SaveMode.Append)
            .save();

        List<String> uris = getUrisInCollection("text-chunks", 4);
        for (String uri : uris) {
            assertTrue(uri.endsWith(".json"));
            JsonNode doc = readJsonDocument(uri);
            assertEquals(JsonNodeType.ARRAY, doc.get("chunks").getNodeType());
        }
    }

    @Test
    void xmlSidecarDocuments() {
        prepareToWriteChunks()
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        List<String> uris = getUrisInCollection("text-chunks", 4);
        for (String uri : uris) {
            assertTrue(uri.endsWith(".xml"));
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementCount("/node()/chunks/chunk", 1);
        }
    }

    private DataFrameWriter<Row> prepareToWriteChunks() {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/marklogic-docs/java-client-intro.txt")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_PREFIX, "/test")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "text-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS);
    }


}
