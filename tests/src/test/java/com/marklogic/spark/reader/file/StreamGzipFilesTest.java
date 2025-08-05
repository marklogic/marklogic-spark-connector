/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StreamGzipFilesTest extends AbstractIntegrationTest {

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
}
