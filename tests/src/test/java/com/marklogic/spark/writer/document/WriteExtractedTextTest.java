/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileCopyUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Iterator;

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
        assertEquals("/extract-test/marklogic-getting-started.pdf", doc.get("source-uri").asText());
        String content = doc.get("content").asText();
        assertTrue(content.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + content);

        JsonNode metadata = doc.get("extracted-metadata");
        // Verify a couple fields as a sanity check.
        assertEquals("Getting Started With MarkLogic Server", metadata.get("pdf-docinfo-title").asText());
        assertEquals("application/pdf", metadata.get("Content-Type").asText());

        Iterator<String> fieldNames = metadata.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            assertFalse(fieldName.contains(":"), "Colons should be replaced with hyphens so that a user can " +
                "optionally create a range index. Field name: " + fieldName);
        }
    }

    @Test
    void microsoftFile() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/hello-world.docx")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*files,'/extract-test'")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/extract-test/hello-world.docx-extracted-text.json");
        assertEquals("/extract-test/hello-world.docx", doc.get("source-uri").asText());
        assertEquals("Hello world.\n\nThis file is used for testing text extraction.\n", doc.get("content").asText());
        assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            doc.get("extracted-metadata").get("Content-Type").asText());
    }

    @Test
    void plainTika() throws Exception {
        File file = new File("src/test/resources/extraction-files/hello-world.docx");
        byte[] bytes = FileCopyUtils.copyToByteArray(file);
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            Metadata metadata = new Metadata();
            String text = new Tika().parseToString(inputStream, metadata);
            assertEquals("Hello world.\n\nThis file is used for testing text extraction.\n", text,
                "The point of this test is to make sure that Tika works by itself with the tika-parser-microsoft-module " +
                    "dependency on the classpath, as we've had some mysterious classpath issues, like the wrong " +
                    "version of commons-compress being used. If this test fails, then we know something is awry " +
                    "with the classpath.");
            assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document", metadata.get("Content-Type"));
        }
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

        doc.assertElementValue("/model:root/model:extracted-metadata/pdf:PDFVersion", "1.5");
        doc.assertElementValue("/model:root/model:extracted-metadata/dc:description", "MarkLogic Server");
    }

    @Test
    void customCollectionsAndPermissions() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option("pathGlobFilter", "*.pdf")
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
            .option("pathGlobFilter", "*.pdf")
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
        assertTrue(doc.has("extracted-metadata"));
        assertFalse(doc.has("source-uri"), "source-uri should be omitted since the source was dropped");
        assertEquals(2, doc.size());
    }

    @Test
    void dropSourceXml() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option("pathGlobFilter", "*.pdf")
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
        doc.assertElementExists("/model:root/model:content");
        doc.assertElementExists("/model:root/model:extracted-metadata");
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

    @Test
    void zipWithEmptyEntry() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/zip-files/empty-entry.zip")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_REPLACE, ".*/zip-files,'/abc'")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "output")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extraction-test")
            .mode(SaveMode.Append)
            .save();

        assertInCollections("/abc/empty-entry.zip/empty.txt", "output");

        String content = getDatabaseClient().newTextDocumentManager().read("/abc/empty-entry.zip/empty.txt", new StringHandle()).get();
        assertNull(content, "The document should be written without error, it just won't have any extracted text.");
    }
}
