/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The generic file reader has support for aborting or continuing on failure - but we haven't yet found a way to
 * force an error to occur. An encoding issue doesn't cause an error because the reader simply reads in all the
 * bytes from the file.
 */
class ReadGenericFilesTest extends AbstractIntegrationTest {

    private static final String ISO_8859_1_ENCODED_FILE = "src/test/resources/encoding/medline.iso-8859-1.txt";

    @Test
    void readAndWriteMixedFiles() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load("src/test/resources/mixed-files");

        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());
        rows.forEach(row -> {
            assertFalse(row.isNullAt(0)); // URI
            assertFalse(row.isNullAt(1)); // content
            Stream.of(2, 3, 4, 5, 6, 7).forEach(index -> assertTrue(row.isNullAt(index),
                "Expecting a null value for every column that isn't URI or content; index: " + index));
        });

        defaultWrite(dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "generic")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''"));

        JsonNode doc = readJsonDocument("/hello.json", "generic");
        assertEquals("world", doc.get("hello").asText());
        XmlNode xmlDoc = readXmlDocument("/hello.xml", "generic");
        xmlDoc.assertElementValue("/hello", "world");
        String text = getDatabaseClient().newTextDocumentManager().read("/hello.txt", new StringHandle()).get();
        assertEquals("hello world", text.trim());
        BytesHandle handle = getDatabaseClient().newBinaryDocumentManager().read("/hello2.txt.gz", new BytesHandle());
        assertEquals(Format.BINARY, handle.getFormat());
    }

    /**
     * Need to actually write the document to force an error to occur.
     */
    @Test
    void wrongEncoding() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load(ISO_8859_1_ENCODED_FILE)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append);

        SparkException ex = assertThrows(SparkException.class, writer::save);
        assertTrue(ex.getMessage().contains("document is not UTF-8 encoded"), "Actual error: " + ex.getMessage());
    }

    @Test
    void customEncoding() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, "ISO-8859-1")
            .load(ISO_8859_1_ENCODED_FILE)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/iso-doc.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/iso-doc.xml");
        doc.assertElementExists("/MedlineCitationSet");
        doc.assertElementValue("/MedlineCitationSet/MedlineCitation/Affiliation",
            "Istituto di Anatomia e Istologia Patologica, Università di Ferrara, Italy.");
    }

    @Test
    void invalidEncodingValue() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, "not-a-real-encoding")
            .load(ISO_8859_1_ENCODED_FILE)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrows(ConnectorException.class, writer::save);
        assertTrue(ex.getMessage().contains("Unsupported encoding value: not-a-real-encoding"), "Actual error: " + ex.getMessage());
    }

    /**
     * Verifies that encoding is applied when a file is gzipped as well. Neat!
     */
    @Test
    void gzippedCustomEncoding() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, "ISO-8859-1")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/encoding/medline2.iso-8859-1.xml.gz")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "encoding-test")
            .mode(SaveMode.Append)
            .save();

        String uri = getUrisInCollection("encoding-test", 1).get(0);
        XmlNode doc = readXmlDocument(uri);
        doc.assertElementExists("/MedlineCitationSet");
    }

    @Test
    void filenameHasSpace() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/generic-files/with-spaces/three uris.csv");

        Row row = dataset.collectAsList().get(0);
        String path = row.getString(0);
        assertTrue(path.endsWith("generic-files/with-spaces/three uris.csv"),
            "The file path should be decoded by default. Under the hood, Spark builds up a set of file paths " +
                "that are URL-encoded. But those will fail when trying to read the file, so they need to be " +
                "decoded. Actual path: " + path);

        String content = new String((byte[]) row.get(1));
        assertEquals("URI\n/process-test1\n/process-test2\n/process-test3\n", content);

        // Write the dataset to verify the URI has a space in it.
        defaultWrite(dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "space-test")
            .option(Options.WRITE_URI_REPLACE, ".*generic-files,''"));

        String uri = getUrisInCollection("space-test", 1).get(0);
        assertEquals("/with-spaces/three uris.csv", uri);
    }

    @Test
    void filenameWithSpaceAndCustomEncoding() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, "ISO-8859-1")
            .load("src/test/resources/generic-files/with-spaces/medline iso 8859 1.txt");

        Row row = dataset.collectAsList().get(0);
        String path = row.getString(0);
        assertTrue(path.endsWith("/with-spaces/medline iso 8859 1.txt"),
            "Verifying that when a custom encoding is specified, the path can still be decoded correctly.");
    }

    @Test
    void filenameWithEncodedSpace() {
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/generic-files/with-spaces")
            .select("URI")
            .orderBy("URI")
            .collectAsList();

        assertEquals(3, rows.size());
        assertTrue(rows.get(0).getString(0).endsWith("/hello%20world.json"));
        assertTrue(rows.get(1).getString(0).endsWith("/medline iso 8859 1.txt"));
        assertTrue(rows.get(2).getString(0).endsWith("/three uris.csv"));
    }

    @Test
    void csvFileWithSpaces() {
        List<Row> rows = newSparkSession().read()
            .option("header", true)
            .csv("src/test/resources/generic-files/with-spaces/three uris.csv")
            .collectAsList();

        assertEquals(3, rows.size(), "This doesn't test our connector, but rather demonstrates that the OOTB " +
            "Spark file data sources correctly handle file paths with spaces in them.");
    }

    @Test
    void jsonFileWithPlusSign() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/generic-files/has+plus.json")
            .collectAsList();

        assertEquals(1, rows.size());
    }
}
