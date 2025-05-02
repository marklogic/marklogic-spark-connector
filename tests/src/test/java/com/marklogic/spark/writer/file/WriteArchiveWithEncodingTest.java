/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteArchiveWithEncodingTest extends AbstractIntegrationTest {

    @Test
    void test(@TempDir Path tempDir) {
        addMetadataToTestDocument();

        // Write the JSON test document to an archive with ISO encoding, including its metadata.
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/utf8-sample.json")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_ENCODING, "ISO-8859-1")
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        // Read the archive with ISO encoding and loading it into MarkLogic.
        sparkSession.read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_ENCODING, "ISO-8859-1")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toAbsolutePath().toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "loaded-data")
            .option(Options.WRITE_URI_PREFIX, "/loaded")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/loaded/utf8-sample.json");
        assertEquals("MaryZhengäöüß??", doc.get("text").asText(), "The value should be mostly the same as the " +
            "original value, except for the last two characters which are replaced when encoded to ISO-8859-1.");

        // Read the loaded document.
        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/loaded/utf8-sample.json")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "metadatavalues")
            .load().collectAsList();

        Map<String, String> metadata = rows.get(0).getJavaMap(7);
        assertEquals("MaryZhengäöüß??", metadata.get("text"), "The user-defined encoding should be applied to " +
            "each metadata entry in the archive file as well. This ensures that the encoding is applied to things " +
            "like metadata values and properties fragments, where a user is free to capture any text they want.");
    }

    /**
     * It's fine to add this to the test document, which is created by the test app. It won't impact any other tests,
     * and it can be run repeatedly without any ill effects.
     */
    private void addMetadataToTestDocument() {
        getDatabaseClient().newServerEval()
            .javascript("declareUpdate(); " +
                "xdmp.documentSetMetadata('/utf8-sample.json', {\"text\": \"MaryZhengäöüß测试\"})")
            .evalAs(String.class);
    }
}
