/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.udf.TextExtractor;
import com.marklogic.spark.udf.TextSplitterConfig;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteExtractedTextTest extends AbstractIntegrationTest {

    private static final UserDefinedFunction TEXT_EXTRACTOR = TextExtractor.build();
    private static final String HELLO_WORLD_DOCUMENT_EXTRACTED_TEXT = "Hello world.\n\nThis file is used for testing text extraction.\n";
    private static final String HELLO_WORLD_DOCUMENT_CHUNK_TEXT = "Hello world.\n\nThis file is used for testing text extraction.";

    @Test
    void defaultToJson() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")));

        assertEquals(2, dataset.count(), "Expecting 2 files from the directory");
        assertEquals(9, dataset.collectAsList().get(0).size(), "Expecting the 8 standard columns for representing a " +
            "column, plus the 'extractedText' column.");

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/extract-test/hello-world.docx-extracted-text.json");
        assertEquals("/extract-test/hello-world.docx", doc.get("source-uri").asText());
        assertEquals(HELLO_WORLD_DOCUMENT_EXTRACTED_TEXT, doc.get("content").asText());

        doc = readJsonDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.json");
        assertEquals("/extract-test/marklogic-getting-started.pdf", doc.get("source-uri").asText());
        String content = doc.get("content").asText();
        assertTrue(content.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + content);
    }

    @Test
    void extractToXml() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/extract-test/hello-world.docx-extracted-text.xml");
        doc.assertElementValue("/model:root/model:source-uri", "/extract-test/hello-world.docx");
        doc.assertElementValue("/model:root/model:content", HELLO_WORLD_DOCUMENT_EXTRACTED_TEXT);

        doc = readXmlDocument("/extract-test/marklogic-getting-started.pdf-extracted-text.xml");
        doc.assertElementValue("/model:root/model:source-uri", "/extract-test/marklogic-getting-started.pdf");
        String content = doc.getElementValue("/model:root/model:content");
        assertTrue(content.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + content);
    }

    @Test
    void customCollectionsAndPermissions() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "original-doc")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extracted-doc")
            .option(Options.WRITE_EXTRACTED_TEXT_PERMISSIONS, DEFAULT_PERMISSIONS + ",qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("original-doc", 2);
        assertCollectionSize("extracted-doc", 2);

        PermissionsTester perms = readDocumentPermissions("/extract-test/hello-world.docx-extracted-text.json");
        assertEquals(2, perms.getDocumentPermissions().size(), "Expecting 2 roles - spark-user-role and qconsole-user");
        perms.assertReadPermissionExists("spark-user-role");
        perms.assertReadPermissionExists("qconsole-user");

        perms = readDocumentPermissions("/extract-test/hello-world.docx");
        assertEquals(1, perms.getDocumentPermissions().size(), "Expecting only spark-user-role");
        perms.assertReadPermissionExists("spark-user-role");
    }

    @Test
    void dropSourceJson() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT_DROP_SOURCE, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should only have the 2 extracted text docs as the two source docs should " +
            "have been dropped", "extraction-test", 2);

        JsonNode doc = readJsonDocument("/extract-test/hello-world.docx-extracted-text.json");
        assertTrue(doc.has("content"));
        assertFalse(doc.has("source-uri"), "source-uri should be omitted since the source was dropped");
        assertEquals(1, doc.size());
    }

    @Test
    void dropSourceXml() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_EXTRACTED_TEXT_DROP_SOURCE, true)
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should only have the 2 extracted text docs as the two source docs should " +
            "have been dropped", "extraction-test", 2);

        XmlNode doc = readXmlDocument("/extract-test/hello-world.docx-extracted-text.xml");
        doc.assertElementExists("/model:root/model:content");
        doc.assertElementMissing("source-uri should be omitted since the source was dropped", "/model:root/model:source-uri");
        doc.assertElementCount("/model:root/node()", 1);
    }

    @Test
    void extractThenSplitToSidecar() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/hello-world.docx")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .withColumn("chunks", new TextSplitterConfig().buildUDF().apply(new Column("extractedText")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/abc'")
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should contain the original binary and the extracted text doc, but not the chunks doc",
            "extraction-test", 2);
        assertInCollections("/abc/hello-world.docx", "extraction-test");

        JsonNode extractedTextDoc = readJsonDocument("/abc/hello-world.docx-extracted-text.json", "extraction-test");
        assertEquals("/abc/hello-world.docx", extractedTextDoc.get("source-uri").asText());
        assertEquals(HELLO_WORLD_DOCUMENT_EXTRACTED_TEXT, extractedTextDoc.get("content").asText());

        assertCollectionSize("chunks", 1);
        JsonNode sidecarDoc = readJsonDocument("/abc/hello-world.docx-extracted-text.json-chunks-1.json", "chunks");

        assertEquals("/abc/hello-world.docx-extracted-text.json", sidecarDoc.get("source-uri").asText());
        assertEquals(1, sidecarDoc.get("chunks").size());
        assertEquals(HELLO_WORLD_DOCUMENT_CHUNK_TEXT, sidecarDoc.get("chunks").get(0).get("text").asText(),
            "The LangChain4j splitter trims each chunk, which results in the trailing newline being removed.");
    }

    @Test
    void extractThenSplitWithChunksInExtractedTextDoc() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/hello-world.docx")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")))
            .withColumn("chunks", new TextSplitterConfig().buildUDF().apply(new Column("extractedText")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/abc'")
            .option(Options.WRITE_COLLECTIONS, "extraction-test")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Should have the original binary doc, and then an extracted text doc containing chunks. " +
                "The chunks are in the extracted text doc since the number of max chunks per sidecar defaults to zero.",
            "extraction-test", 2);

        JsonNode doc = readJsonDocument("/abc/hello-world.docx-extracted-text.json");
        assertEquals(3, doc.size(), "Should have source-uri, content, and chunks.");
        assertEquals("/abc/hello-world.docx", doc.get("source-uri").asText());
        assertEquals(HELLO_WORLD_DOCUMENT_EXTRACTED_TEXT, doc.get("content").asText());
        assertEquals(HELLO_WORLD_DOCUMENT_CHUNK_TEXT, doc.get("chunks").get(0).get("text").asText());
    }

    @Test
    void invalidColumn() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("uri")));

        SparkException ex = assertThrows(SparkException.class, () -> dataset.collectAsList());
        assertTrue(ex.getMessage().contains("Text extraction UDF must be run against a column containing non-null byte arrays."),
            "Unexpected error: " + ex.getMessage());
    }

}
