/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void twoJsonPointersWithCustomJoinDelimiter() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text\n/more-text")
            .option(Options.WRITE_SPLITTER_JOIN_DELIMITER, "---")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(2, doc.get("chunks").size(), "Expecting 2 chunks based on default max chunk size of 100.");
        String lastChunk = doc.get("chunks").get(1).get("text").asText();
        assertTrue(lastChunk.endsWith("Choose a REST API Instance.---Hello world."), "The last chunk should " +
            "end with the '/more-text' concatenated to the end of the '/text/' text, using the custom " +
            "join delimiter. Actual chunk: " + lastChunk);
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

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
