/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SplitJsonDocumentTest extends AbstractIntegrationTest {

    @Test
    void oneJsonPointer() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(2, doc.get("chunks").size(), "Expecting 2 chunks based on default max chunk size of 1000.");
        assertTrue(doc.get("chunks").get(0).has("text"));
        assertTrue(doc.get("chunks").get(1).has("text"));
    }

    @Test
    void twoJsonPointers() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on max chunk size of 500.");
        String lastChunk = doc.get("chunks").get(3).get("text").asText();
        assertTrue(lastChunk.endsWith("Choose a REST API Instance. Hello world."), "The last chunk should contain " +
            "the last bits of text in the '/text' path, plus the text in the '/more-text' path, concatenated " +
            "together with a string. Actual chunk: " + lastChunk);
    }

    @Test
    void arrayDoc() {
        readDocument("/marklogic-docs/array-doc.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc instanceof ArrayNode);
        assertEquals("Hello world.", doc.get(0).get("text").asText(),
            "We currently don't support any matching on an array. So any expression is going to return no text, " +
                "thus resulting in no splitting. So the output document will be the same as the input.");
    }

    @Test
    void invalidJsonPointer() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "not-valid")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Unable to use JSON pointer expression: not-valid; cause: Invalid input: " +
                "JSON Pointer expression must start with '/': \"not-valid\"",
            ex.getMessage());
    }

    @Test
    void maxChunksOfThree() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertFalse(doc.has("chunks"), "The source document should not be modified since max chunks is greater than " +
            "zero, which means chunks should be added to one or more sidecar documents.");

        assertCollectionSize("2 chunk documents should have been created, as 4 chunks were created and " +
            "the max chunk count per document is 3. So the first chunk doc should have 3 chunks, and the second " +
            "should have 1 chunk.", "chunks", 2);

        JsonNode firstChunkDoc = readJsonDocument("/split-test.json-chunks-0.json");
        assertEquals("/split-test.json", firstChunkDoc.get("source-uri").asText());
        assertEquals(3, firstChunkDoc.get("chunks").size());

        JsonNode secondChunkDoc = readJsonDocument("/split-test.json-chunks-1.json");
        assertEquals("/split-test.json", secondChunkDoc.get("source-uri").asText());
        assertEquals(1, secondChunkDoc.get("chunks").size());

        PermissionsTester tester = readDocumentPermissions("/split-test.json-chunks-0.json");
        tester.assertReadPermissionExists("The chunk documents should default to the permissions specified " +
            "via Options.WRITE_PERMISSIONS", "spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
    }

    @Test
    void maxChunksWithCustomPermissions() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS,
                "spark-user-role,read,spark-user-role,update,qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester tester = readDocumentPermissions("/split-test.json-chunks-0.json");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
    }

    @Test
    void maxChunksWithCustomUri() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_OUTPUT_URI_PREFIX, "/chunk/")
            .option(Options.WRITE_SPLITTER_OUTPUT_URI_SUFFIX, ".json")
            .mode(SaveMode.Append)
            .save();

        getUrisInCollection("chunks", 2).forEach(uri -> {
            assertTrue(uri.startsWith("/chunk/"), "Unexpected URI: " + uri);
            assertTrue(uri.endsWith(".json"), "Unexpected URI: " + uri);
            JsonNode doc = readJsonDocument(uri);
            assertEquals("/split-test.json", doc.get("source-uri").asText());
            assertEquals(2, doc.get("chunks").size());
        });
    }

    @Test
    void maxChunksWithCustomRootName() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_OUTPUT_ROOT_NAME, "sidecar")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-0.json");
        assertTrue(doc.has("sidecar"));
        assertEquals("/split-test.json", doc.get("sidecar").get("source-uri").asText());
        assertEquals(4, doc.get("sidecar").get("chunks").size(), "The sidecar document should have all 4 chunks in " +
            "it.");
        assertCollectionSize("Should only have one document as all 4 chunks fit in it", "chunks", 1);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
