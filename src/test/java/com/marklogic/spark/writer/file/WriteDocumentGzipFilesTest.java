/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteDocumentGzipFilesTest extends AbstractIntegrationTest {

    @Test
    void test(@TempDir Path tempDir) {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "gzip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyFifteenGZIPFilesWereWritten(tempDir);
        verifyGZippedFilesCanBeReadAndWrittenToMarkLogic(tempDir);
    }

    private void verifyFifteenGZIPFilesWereWritten(Path tempDir) {
        File dir = tempDir.toFile();
        assertEquals(1, dir.listFiles().length, "Expecting a single 'author' directory");

        File authorDir = new File(dir, "author");
        assertEquals(15, authorDir.listFiles().length);

        for (File file : authorDir.listFiles()) {
            assertTrue(file.getName().endsWith(".json.gz"), "Unexpected filename: " + file.getName());
        }
    }

    private void verifyGZippedFilesCanBeReadAndWrittenToMarkLogic(Path tempDir) {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load(new File(tempDir.toFile(), "author").getAbsolutePath());

        assertEquals(15, dataset.count());

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/author,''")
            .option(Options.WRITE_URI_PREFIX, "/gzip-test/")
            .option(Options.WRITE_COLLECTIONS, "gzip-test")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("gzip-test", 15);
        for (int i = 1; i <= 15; i++) {
            String expectedUri = String.format("/gzip-test/%d.json", i);
            JsonNode doc = readJsonDocument(expectedUri);
            assertTrue(doc.has("CitationID"));
        }
    }
}
