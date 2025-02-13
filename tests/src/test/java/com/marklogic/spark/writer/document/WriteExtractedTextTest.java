/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteExtractedTextTest extends AbstractIntegrationTest {

    @Test
    void defaultToJson() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-getting-started.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.json");
        System.out.println(doc.toPrettyString());
        assertEquals("/extract-test/marklogic-getting-started.pdf", doc.get("source-uri").asText());
        String content = doc.get("content").asText();
        assertTrue(content.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + content);
    }

    @Test
    void extractToXml() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-getting-started.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.xml");
        doc.assertElementValue("/model:root/model:source-uri", "/extract-test/marklogic-getting-started.pdf");
        String content = doc.getElementValue("/model:root/model:content");
        assertTrue(content.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + content);
    }

    @Test
    void customCollectionsAndPermissions() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "original-doc")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extracted-doc")
            .option(Options.WRITE_EXTRACTED_TEXT_PERMISSIONS, DEFAULT_PERMISSIONS + ",qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("original-doc", 2);
        assertCollectionSize("extracted-doc", 2);

        PermissionsTester perms = readDocumentPermissions("/extract-test/marklogic-getting-started.pdf-extracted-text.json");
        assertEquals(2, perms.getDocumentPermissions().size(), "Expecting 2 roles - spark-user-role and qconsole-user");
        perms.assertReadPermissionExists("spark-user-role");
        perms.assertReadPermissionExists("qconsole-user");

        perms = readDocumentPermissions("/extract-test/marklogic-getting-started.pdf");
        assertEquals(1, perms.getDocumentPermissions().size(), "Expecting only spark-user-role");
        perms.assertReadPermissionExists("spark-user-role");
    }

    @Test
    void customCollectionsWithoutPermissions() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-getting-started.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "original-doc")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extracted-doc")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester perms = readDocumentPermissions("/extract-test/marklogic-getting-started.pdf-extracted-text.json");
        final String message = "The extracted text doc should default to " + Options.WRITE_PERMISSIONS + " when " +
            "extracted text permissions are not specified.";
        perms.assertReadPermissionExists(message, "spark-user-role");
        perms.assertUpdatePermissionExists(message, "spark-user-role");
    }

    @Test
    void dropSourceJson() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_EXTRACTED_TEXT_DROP_SOURCE, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should only have the 2 extracted text docs as the two source docs should " +
            "have been dropped", "extraction-test", 2);

        JsonNode doc = readJsonDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.json");
        assertTrue(doc.has("content"));
        assertTrue(doc.has("metadata"));
        assertFalse(doc.has("source-uri"), "source-uri should be omitted since the source was dropped");
        assertEquals(2, doc.size());
    }

    @Test
    void dropSourceXml() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_EXTRACTED_TEXT_DROP_SOURCE, true)
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should only have the 2 extracted text docs as the two source docs should " +
            "have been dropped", "extraction-test", 2);

        XmlNode doc = readXmlDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.xml");
        System.out.println(doc.getPrettyXml());
        doc.assertElementExists("/model:root/model:content");
        doc.assertElementExists("/model:root/model:metadata");
        doc.assertElementMissing("source-uri should be omitted since the source was dropped", "/model:root/model:source-uri");
        doc.assertElementCount("/model:root/node()", 2);
    }

    @Test
    void extractThenSplitToSidecar() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-getting-started.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/abc'")
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 10000)
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extraction-test")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should contain the original binary and the extracted text doc, but not the chunks doc",
            "extraction-test", 2);
        assertInCollections("/abc/marklogic-getting-started.pdf", "extraction-test");

        JsonNode extractedTextDoc = readJsonDocument("/abc/marklogic-getting-started.pdf-extracted-text.json", "extraction-test");
        assertEquals("/abc/marklogic-getting-started.pdf", extractedTextDoc.get("source-uri").asText());
        assertTrue(extractedTextDoc.get("content").asText().contains("MarkLogic Server Table of Contents"));

        assertCollectionSize("Expecting 5 chunks based on a max chunk size of 10k chars", "chunks", 5);
        JsonNode sidecarDoc = readJsonDocument("/abc/marklogic-getting-started.pdf-extracted-text.json-chunks-1.json", "chunks");

        assertEquals("/abc/marklogic-getting-started.pdf-extracted-text.json", sidecarDoc.get("source-uri").asText());
        assertEquals(1, sidecarDoc.get("chunks").size());
        assertTrue(sidecarDoc.get("chunks").get(0).get("text").asText().contains("MarkLogic Server Table of Contents"));
    }

    @Test
    void extractThenSplitWithChunksInExtractedTextDoc() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-getting-started.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/abc'")
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extraction-test")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should have the original binary doc, and then an extracted text doc containing chunks. " +
                "The chunks are in the extracted text doc since the number of max chunks per sidecar defaults to zero.",
            "extraction-test", 2);

        JsonNode doc = readJsonDocument("/abc/marklogic-getting-started.pdf-extracted-text.json");
        assertEquals(4, doc.size(), "Should have source-uri, content, metadata, and chunks.");
        assertEquals("/abc/marklogic-getting-started.pdf", doc.get("source-uri").asText());
        assertTrue(doc.get("content").asText().contains("MarkLogic Server Table of Contents"));
        assertTrue(doc.get("chunks").get(0).get("text").asText().contains("MarkLogic Server Table of Contents"));
    }
}
