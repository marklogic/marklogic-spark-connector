/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests are simpler than they look at first glance. Each one reads a doc from MarkLogic that contains characters
 * supported by UTF-8 but not supported by ISO-8859-1. The test then writes the doc to a file using ISO-8859-1. It then
 * reads the file and loads it back into MarkLogic and verifies that the contents of both the written file and written
 * document meet the expectations for ISO-8859-1 encoding.
 */
class WriteFilesWithEncodingTest extends AbstractIntegrationTest {

    private static final String ISO_ENCODING = "ISO-8859-1";
    private static final String SAMPLE_XML_DOC_URI = "/utf8-sample.xml";
    private static final String SAMPLE_JSON_DOC_URI = "/utf8-sample.json";
    private static final String ORIGINAL_XML_TEXT = "UTF-8 Text: MaryZhengäöüß测试";

    @Test
    void writeXmlFile(@TempDir Path tempDir) {
        XmlNode sampleDoc = readXmlDocument(SAMPLE_XML_DOC_URI);
        sampleDoc.assertElementValue(
            "Verifying that the sample doc was loaded correctly in the test app; also showing what the text looks " +
                "to make this test easier to understand.",
            "/doc", ORIGINAL_XML_TEXT);

        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, SAMPLE_XML_DOC_URI)
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_ENCODING, ISO_ENCODING)
            .mode(SaveMode.Append)
            .save(tempDir.toAbsolutePath().toString());

        String fileContent = readFileContents(tempDir, "utf8-sample.xml");
        assertTrue(fileContent.contains("<doc>UTF-8 Text: MaryZheng����??</doc>"),
            "Unexpected file content: " + fileContent);

        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, ISO_ENCODING)
            .load(tempDir.toAbsolutePath().toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/iso-doc.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/iso-doc.xml");
        doc.assertElementValue(
            "Verifies that the ISO-encoded text is then converted back to UTF-8 when stored in MarkLogic, but the " +
                "value is slightly different due to the use of replacement characters in ISO-8859-1.",
            "/doc", "UTF-8 Text: MaryZhengäöüß??");
    }

    @Test
    void prettyPrintXmlFile(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, SAMPLE_XML_DOC_URI)
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_ENCODING, ISO_ENCODING)
            .option(Options.WRITE_FILES_PRETTY_PRINT, true)
            .mode(SaveMode.Append)
            .save(tempDir.toAbsolutePath().toString());

        String fileContent = readFileContents(tempDir, "utf8-sample.xml");
        assertTrue(fileContent.contains("<doc>UTF-8 Text: MaryZheng����&#27979;&#35797;</doc>"),
            "Pretty-printing results in some of the characters being escaped by the Java Transformer class, " +
                "even though it's been configured to use the user-specified encoding. Unexpected text: " + fileContent);

        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, ISO_ENCODING)
            .load(tempDir.toAbsolutePath().toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/iso-doc.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/iso-doc.xml");
        doc.assertElementValue(
            "The written doc should have the original XML text, as the problematic characters for ISO-8859-1 were " +
                "escaped by the Java Transformer class during the pretty-printing process. This shows that " +
                "pretty-printing can actually result in fewer characters being altered via replacement tokens.",
            "/doc", ORIGINAL_XML_TEXT);
    }

    @Test
    void prettyPrintJsonFile(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, SAMPLE_JSON_DOC_URI)
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_ENCODING, ISO_ENCODING)
            .option(Options.WRITE_FILES_PRETTY_PRINT, true)
            .mode(SaveMode.Append)
            .save(tempDir.toAbsolutePath().toString());

        String fileContent = readFileContents(tempDir, "utf8-sample.json");
        assertTrue(fileContent.contains("MaryZheng����??"),
            "Pretty-printing JSON doesn't impact the encoding at all since the underlying Jackson library " +
                "doesn't need to escape any of the characters. Unexpected text: " + fileContent);

        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, ISO_ENCODING)
            .load(tempDir.toAbsolutePath().toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/iso-doc.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/iso-doc.json");
        assertEquals("MaryZhengäöüß??", doc.get("text").asText());
    }

    @Test
    void invalidEncoding() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, SAMPLE_JSON_DOC_URI)
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_ENCODING, "not-valid-encoding")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save("."));
        assertEquals("Unsupported encoding value: not-valid-encoding", ex.getMessage());
    }

    private String readFileContents(Path tempDir, String filename) {
        File file = new File(tempDir.toFile(), filename);
        try {
            return new String(FileCopyUtils.copyToByteArray(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
