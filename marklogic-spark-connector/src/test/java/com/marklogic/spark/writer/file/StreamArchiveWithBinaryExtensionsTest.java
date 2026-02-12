/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the STREAM_TRANSFORM_BINARY_EXTENSIONS option to selectively apply transforms during
 * streaming import based on document format and URI extension.
 */
class StreamArchiveWithBinaryExtensionsTest extends AbstractIntegrationTest {

    private static final String NORMAL_JSON_URI = "/author/author1.json";
    private static final String BINARY_JSON_URI = "/test/binary.json";
    private static final String BINARY_DOC_URI = "/binary/hello-world.docx";

    @BeforeEach
    void setupThreeDocumentsToExportToArchive() {
        // Doc 1: Use existing JSON document (/author/author1.json) - already in database
        // Doc 3: Use existing binary document (/binary/hello-world.docx) - already in database
        // Doc 2: Create a JSON document as BINARY using the toBinary transform
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files/hello.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_REPLACE, ".*hello.json,'" + BINARY_JSON_URI + "'")
            .option(Options.WRITE_TRANSFORM_NAME, "toBinary")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        // Verify the setup
        assertEquals("JSON", readDocumentFormat(NORMAL_JSON_URI), "Doc 1 should be JSON");
        assertEquals("BINARY", readDocumentFormat(BINARY_JSON_URI), "Doc 2 should be BINARY");
        assertEquals("BINARY", readDocumentFormat(BINARY_DOC_URI), "Doc 3 should be BINARY");
    }

    @Test
    void importArchiveWithoutBinaryExtensionsOption(@TempDir Path tempDir) {
        exportTheThreeDocumentsToAnArchive(tempDir);

        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.STREAM_FILES, true)
            .load(tempDir.toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .option(Options.WRITE_URI_PREFIX, "/binary-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "binary-test")
            .mode(SaveMode.Append)
            .save();

        // Without the transform, all documents should be written based on their URI extension. Note that
        // BINARY_JSON_URI is no longer a BINARY.
        assertEquals("JSON", readDocumentFormat("/binary-test" + NORMAL_JSON_URI));
        assertEquals("JSON", readDocumentFormat("/binary-test" + BINARY_JSON_URI));
        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_DOC_URI));
    }

    @Test
    void importArchiveWithBinaryExtensionsOption(@TempDir Path tempDir) {
        exportTheThreeDocumentsToAnArchive(tempDir);

        // Import archive with STREAM_TRANSFORM_BINARY_EXTENSIONS=json
        // Transform should ONLY be applied to documents with format=BINARY and URI ending in .json
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.STREAM_FILES, true)
            .load(tempDir.toString())
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .option(Options.STREAM_TRANSFORM_BINARY_EXTENSIONS, "json")
            .option(Options.WRITE_TRANSFORM_NAME, "toBinary")
            .option(Options.WRITE_URI_PREFIX, "/binary-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "binary-test")
            .mode(SaveMode.Append)
            .save();

        // With the option, transform should only be applied to doc 2 (format=BINARY + .json extension)
        assertEquals("JSON", readDocumentFormat("/binary-test" + NORMAL_JSON_URI),
            "Doc 1 (normal JSON with format=JSON) should remain JSON - transform not applied");
        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_JSON_URI),
            "Doc 2 (format=BINARY with .json extension) should be BINARY - transform was applied");
        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_DOC_URI),
            "Doc 3 (actual binary without .json extension) should remain BINARY - transform not applied");
    }

    private void exportTheThreeDocumentsToAnArchive(Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, NORMAL_JSON_URI + "\n" + BINARY_JSON_URI + "\n" + BINARY_DOC_URI)
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());
    }

    private String readDocumentFormat(String uri) {
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        return rows.get(0).getString(2);
    }
}
