/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.nuclia;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessWithNucliaTest extends AbstractIntegrationTest {

    private static final String MOCK_RESPONSE = """
        {
            "type": "Chunk",
            "text": "The chunk text",
             "metadata": {
                "meta1": "value1",
                "meta2": "value2"
             },
             "embeddings": [{
                "id": "multilingual-2024-05-06",
                "embedding": [0.123, -0.456]
             }]
        }
        """;

    @Test
    void jsonChunk() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option("spark.marklogic.testing.mockNuaClientResponse", MOCK_RESPONSE)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "nuclia-chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("nuclia-chunks", 1);

        final String expectedChunkUri = "/split-test.json-extracted-text.json-chunks-1.json";
        JsonNode doc = readJsonDocument(expectedChunkUri);
        assertEquals("/split-test.json-extracted-text.json", doc.get("source-uri").asText());
        assertEquals(1, doc.get("chunks").size());

        JsonNode chunk = doc.get("chunks").get(0);
        assertEquals("The chunk text", chunk.get("text").asText());
        assertEquals("value1", chunk.get("chunk-metadata").get("meta1").asText());
        assertEquals("value2", chunk.get("chunk-metadata").get("meta2").asText());
        assertEquals("multilingual-2024-05-06", chunk.get("model-name").asText());
        assertEquals(2, chunk.get("_vector").size());
        assertEquals(0.123, chunk.get("_vector").get(0).asDouble());
        assertEquals(-0.456, chunk.get("_vector").get(1).asDouble());

        PermissionsTester perms = readDocumentPermissions(expectedChunkUri);
        perms.assertReadPermissionExists("spark-user-role");
        perms.assertUpdatePermissionExists("spark-user-role");
    }

    @Test
    void xmlChunk() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option("spark.marklogic.testing.mockNuaClientResponse", MOCK_RESPONSE)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "nuclia-chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("nuclia-chunks", 1);

        final String expectedChunkUri = "/split-test.json-extracted-text.json-chunks-1.xml";
        XmlNode doc = readXmlDocument(expectedChunkUri);
        doc.assertElementValue("/model:root/model:source-uri", "/split-test.json-extracted-text.json");
        doc.assertElementCount("/model:root/model:chunks/model:chunk", 1);
        doc.assertElementValue("/model:root/model:chunks/model:chunk[1]/model:text", "The chunk text");
        doc.assertElementValue("/model:root/model:chunks/model:chunk[1]/vec:model-name", "multilingual-2024-05-06");
        doc.assertElementValue("/model:root/model:chunks/model:chunk[1]/vec:vector[@xml:lang='zxx']", "[0.123, -0.456]");

        doc.assertElementValue(
            "The Nuclia metadata JSON is stored as a serialized string as there's not a standard approach for " +
                "serializing JSON into XML. If the user wants it to be XML, they can use a REST transform to get the " +
                "desired structure for the XML.",
            "/model:root/model:chunks/model:chunk[1]/model:chunk-metadata",
            "{\"meta1\":\"value1\",\"meta2\":\"value2\"}"
        );
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
