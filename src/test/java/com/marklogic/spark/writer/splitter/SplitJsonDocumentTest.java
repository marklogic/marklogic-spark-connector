/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
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
    void jsonPointerEntireDoc() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000)
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        System.out.println(doc.toPrettyString());

        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        assertEquals(3, chunks.size(), "Expecting 3 chunks based on the entire serialized doc and a size of 1000.");

        String firstChunk = chunks.get(0).get("text").asText();
        assertTrue(firstChunk.startsWith("{\"url\":\"https"), "The expression '\"\"' is a valid JSON Pointer " +
            "expression that refers to the entire doc. So the first chunk should begin with the serialization of " +
            "the entire doc. Actual first chunk: " + firstChunk);
    }

    @Test
    void jsonPointerArray() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/test-array")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 100)
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        assertEquals(2, chunks.size(), "Expecting 2 chunks based on the size of 100 and the amount of text in " +
            "the entire /test-array array.");

        String firstChunk = chunks.get(0).get("text").asText();
        assertTrue(firstChunk.startsWith("[\"When working with the Java API"), "When a JSON Pointer expression " +
            "selects an entire array, the serialized array should be used as the selected text. If a user wants a " +
            "specific value, their expression should select it via its array index. Actual chunk: " + firstChunk);

        String secondChunk = chunks.get(1).get("text").asText();
        assertTrue(secondChunk.endsWith("you want to perform.\"}]"), "The second chunk should end with the " +
            "serialized array ending. Actual chunk: " + secondChunk);
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
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertFalse(doc.has("chunks"), "The source document should not be modified since max chunks is greater than " +
            "zero, which means chunks should be added to one or more sidecar documents.");

        assertCollectionSize("2 chunk documents should have been created, as 4 chunks were created and " +
            "the max chunk count per document is 3. So the first chunk doc should have 3 chunks, and the second " +
            "should have 1 chunk.", "chunks", 2);

        JsonNode firstChunkDoc = readJsonDocument("/split-test.json-chunks-1.json");
        assertEquals("/split-test.json", firstChunkDoc.get("source-uri").asText());
        assertEquals(3, firstChunkDoc.get("chunks").size());

        JsonNode secondChunkDoc = readJsonDocument("/split-test.json-chunks-2.json");
        assertEquals("/split-test.json", secondChunkDoc.get("source-uri").asText());
        assertEquals(1, secondChunkDoc.get("chunks").size());

        PermissionsTester tester = readDocumentPermissions("/split-test.json-chunks-1.json");
        tester.assertReadPermissionExists("The chunk documents should default to the permissions specified " +
            "via Options.WRITE_PERMISSIONS", "spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
    }

    @Test
    void maxChunksWithCustomPermissions() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS,
                "spark-user-role,read,spark-user-role,update,qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester tester = readDocumentPermissions("/split-test.json-chunks-1.json");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
    }

    @Test
    void maxChunksWithCustomUri() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_URI_PREFIX, "/chunk/")
            .option(Options.WRITE_SPLITTER_SIDECAR_URI_SUFFIX, ".json")
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
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME, "sidecar")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.has("sidecar"));
        assertEquals("/split-test.json", doc.get("sidecar").get("source-uri").asText());
        assertEquals(4, doc.get("sidecar").get("chunks").size(), "The sidecar document should have all 4 chunks in " +
            "it.");
        assertCollectionSize("Should only have one document as all 4 chunks fit in it", "chunks", 1);
    }

    /**
     * Demonstrates that XML chunk documents can be written even when the source document is JSON.
     */
    @Test
    void xmlChunks() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("chunks", 2);

        XmlNode doc = readXmlDocument("/split-test.json-chunks-1.xml");
        doc.assertElementCount("/root/chunks/chunk", 2);

        doc = readXmlDocument("/split-test.json-chunks-2.xml");
        doc.assertElementCount("/root/chunks/chunk", 2);
    }

    @Test
    void customSplitter() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS, "com.marklogic.spark.writer.splitter.CustomSplitter")
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS_OPTION_PREFIX + "textToReturn", "this is a test")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals("this is a test", doc.at("/chunks/0/text").asText(),
            "The custom splitter should have been used, which results in a single chunk being added with the " +
                "value of the 'textToReturn' option used as the chunk text.");
    }

    @Test
    void customSplitterNoClassOptions() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS, "com.marklogic.spark.writer.splitter.CustomSplitter")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals("You passed in null!", doc.at("/chunks/0/text").asText());
    }

    @Test
    void customSplitterInvalidClass() {
        DataFrameWriter writer = prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS, "not.valid")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Cannot find custom splitter with class name: not.valid", ex.getMessage());
    }

    @Test
    void customSplitterNotADocumentSplitter() {
        DataFrameWriter writer = prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS, "com.marklogic.spark.writer.splitter.BadCustomSplitter")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Cannot create custom splitter with class name: com.marklogic.spark.writer.splitter.BadCustomSplitter; " +
                "the class must have a public constructor that accepts a java.util.Map<String, String>.",
            ex.getMessage());
    }

    @Test
    void chunksFieldAlreadyExists() {
        readDocument("/marklogic-docs/has-chunks-already.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals("already exists", doc.get("chunks").asText(), "If a top-level 'chunks' field already exists, " +
            "it should not be modified.");

        ArrayNode chunks = (ArrayNode) doc.get("splitter-chunks");
        assertEquals(1, chunks.size(), "If a top-level 'chunks' field exists, then the connector should " +
            "use 'splitter-chunks' as a name. The expectation is that this name is extremely unlikely to be in use " +
            "already, such that we do not need to provide a configuration option for defining the chunks array name.");
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private DataFrameWriter prepareToWriteChunkDocuments() {
        return readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json");
    }
}
