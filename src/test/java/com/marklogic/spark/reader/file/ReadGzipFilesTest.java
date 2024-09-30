/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadGzipFilesTest extends AbstractIntegrationTest {

    @Test
    void readThreeGZIPFiles() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option("recursiveFileLookup", "true")
            .load("src/test/resources/gzip-files")
            .collectAsList();

        assertEquals(3, rows.size());

        verifyRow(rows.get(0), "/src/test/resources/gzip-files/hello.xml", "<hello>world</hello>\n");
        verifyRow(rows.get(1), "/src/test/resources/gzip-files/level1/hello.txt", "hello world\n");
        verifyRow(rows.get(2), "/src/test/resources/gzip-files/level1/level2/hello world.json", "{\"hello\":\"world\"}\n");
    }

    @Test
    void threeFilesOnePartition() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option("recursiveFileLookup", "true")
            .load("src/test/resources/gzip-files")
            .collectAsList();

        assertEquals(3, rows.size());
    }

    @Test
    void filesNotGzipped() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/zip-files/mixed-files.zip");

        SparkException ex = assertThrows(SparkException.class, () -> dataset.count());
        assertTrue(ex.getCause() instanceof ConnectorException);
        assertTrue(ex.getCause().getMessage().startsWith("Unable to read file at file:///"),
            "Unexpected error message: " + ex.getCause().getMessage());
    }

    @Test
    void dontAbortOnFailure() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .option("recursiveFileLookup", true)
            .load("src/test/resources/zip-files/mixed-files.zip", "src/test/resources/gzip-files")
            .collectAsList();

        assertEquals(3, rows.size(), "Expecting to get the 3 files back from the gzip-files directory, with the " +
            "error for the non-gzipped mixed-files.zip file being logged as a warning but not causing a failure.");
    }

    @Test
    void streamThreeGZIPFiles() throws Exception {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option("recursiveFileLookup", "true")
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/gzip-files");

        List<Row> rows = dataset.collectAsList();
        assertEquals(3, rows.size());
        for (Row row : rows) {
            assertFalse(row.isNullAt(0), "The URI column should be populated.");
            byte[] content = (byte[]) row.get(1);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(content))) {
                FileContext fileContext = (FileContext) ois.readObject();
                assertNotNull(fileContext);
            }
        }

        // Write the streaming files to MarkLogic.
        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "streamed-files")
            .option(Options.WRITE_URI_REPLACE, ".*gzip-files,'/gzip-files'")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("streamed-files", 3);
        XmlNode doc = readXmlDocument("/gzip-files/hello.xml");
        doc.assertElementValue("/hello", "world");

        // Because each streamed file has to be sent via a PUT request, and the PUT endpoint does not allow spaces -
        // see MLE-17088 - the URI will be encoded.
        JsonNode node = readJsonDocument("/gzip-files/level1/level2/hello%20world.json");
        assertEquals("world", node.get("hello").asText());
    }

    private void verifyRow(Row row, String expectedUriSuffix, String expectedContent) {
        String uri = row.getString(0);
        assertTrue(uri.endsWith(expectedUriSuffix), "Unexpected URI: " + uri);
        String content = new String((byte[]) row.get(1));
        assertEquals(expectedContent, content);
    }
}
