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
 * <p>
 * All document formats (JSON, XML, BINARY, TEXT) are consistently encoded in metadata entry names
 * during both streaming and non-streaming exports for consistency and potential future use.
 */
class StreamArchiveWithBinaryExtensionsTest extends AbstractIntegrationTest {

    private static final String NORMAL_JSON_URI = "/author/author1.json";
    private static final String BINARY_JSON_URI = "/test/binary.json";
    private static final String TEXT_JSON_URI = "/test/text.json";
    private static final String BINARY_DOC_URI = "/binary/hello-world.docx";

    @BeforeEach
    void setupFourDocumentsToExportToArchive() {
        // Doc 1: Use existing JSON document (/author/author1.json) - already in database
        // Doc 2: Create a JSON document as BINARY using the toBinary transform
        // Doc 3: Create a JSON document as TEXT using the toText transform
        // Doc 4: Use existing binary document (/binary/hello-world.docx) - already in database
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files/hello.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_REPLACE, ".*hello.json,'" + BINARY_JSON_URI + "'")
            .option(Options.WRITE_TRANSFORM_NAME, "toBinary")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files/hello.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_REPLACE, ".*hello.json,'" + TEXT_JSON_URI + "'")
            .option(Options.WRITE_TRANSFORM_NAME, "toText")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        // Verify the setup
        assertEquals("JSON", readDocumentFormat(NORMAL_JSON_URI), "Doc 1 should be JSON");
        assertEquals("BINARY", readDocumentFormat(BINARY_JSON_URI), "Doc 2 should be BINARY");
        assertEquals("TEXT", readDocumentFormat(TEXT_JSON_URI), "Doc 3 should be TEXT");
        assertEquals("BINARY", readDocumentFormat(BINARY_DOC_URI), "Doc 4 should be BINARY");
    }

    @Test
    void withoutBinaryExtensionsOption(@TempDir Path tempDir) {
        exportTheFourDocumentsToAnArchive(tempDir);

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

        // Without the transform, all documents should be written based on their URI extension.
        // Note that BINARY_JSON_URI is no longer BINARY, and TEXT_JSON_URI becomes JSON (no "text extensions" option).
        // Even though formats are encoded in metadata, streaming import uses URI extension without specific options.
        assertEquals("JSON", readDocumentFormat("/binary-test" + NORMAL_JSON_URI));
        assertEquals("JSON", readDocumentFormat("/binary-test" + BINARY_JSON_URI));
        assertEquals("JSON", readDocumentFormat("/binary-test" + TEXT_JSON_URI));
        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_DOC_URI));
    }

    @Test
    void withBinaryExtensionsOption(@TempDir Path tempDir) {
        exportTheFourDocumentsToAnArchive(tempDir);

        importArchiveWithBinaryExtensionsOption(tempDir);

        verifyBinaryJsonHasBinaryFormat();
    }

    @Test
    void withBinaryExtensionsOptionAndExportArchiveViaStreaming(@TempDir Path tempDir) {
        exportTheFourDocumentsByStreamingToAnArchive(tempDir);

        importArchiveWithBinaryExtensionsOption(tempDir);

        verifyBinaryJsonHasBinaryFormat();
    }

    @Test
    void nonStreamingReadPreservesFormat(@TempDir Path tempDir) {
        exportTheFourDocumentsToAnArchive(tempDir);

        // Read archive without streaming - format information should be preserved from metadata. It is up to the
        // user though to use a REST transform that can account for the format.
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toString())
            .orderBy("uri")
            .collectAsList();

        assertEquals(4, rows.size());
        assertEquals("/author/author1.json", rows.get(0).getString(0));
        assertEquals("JSON", rows.get(0).getString(2));
        assertEquals("/binary/hello-world.docx", rows.get(1).getString(0));
        assertEquals("BINARY", rows.get(1).getString(2));
        assertEquals("/test/binary.json", rows.get(2).getString(0));
        assertEquals("BINARY", rows.get(2).getString(2));
        assertEquals("/test/text.json", rows.get(3).getString(0));
        assertEquals("TEXT", rows.get(3).getString(2));
    }

    private void exportTheFourDocumentsToAnArchive(Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, NORMAL_JSON_URI + "\n" + BINARY_JSON_URI + "\n" + TEXT_JSON_URI + "\n" + BINARY_DOC_URI)
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());
    }

    private void exportTheFourDocumentsByStreamingToAnArchive(Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, NORMAL_JSON_URI + "\n" + BINARY_JSON_URI + "\n" + TEXT_JSON_URI + "\n" + BINARY_DOC_URI)
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());
    }

    private void importArchiveWithBinaryExtensionsOption(Path tempDir) {
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
    }

    private void verifyBinaryJsonHasBinaryFormat() {
        // With the binary extension option, transform should only be applied to doc 2 (format=BINARY + .json extension).
        // TEXT_JSON_URI is now encoded in metadata but still becomes JSON on import (no "text extensions" option).
        assertEquals("JSON", readDocumentFormat("/binary-test" + NORMAL_JSON_URI),
            "Doc 1 (normal JSON with format=JSON) should remain JSON - transform not applied");

        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_JSON_URI),
            "Doc 2 (format=BINARY with .json extension) should be BINARY - transform was applied");

        assertEquals("JSON", readDocumentFormat("/binary-test" + TEXT_JSON_URI),
            "Doc 3 (format=TEXT with .json extension) should become JSON - no text extensions option");

        assertEquals("BINARY", readDocumentFormat("/binary-test" + BINARY_DOC_URI),
            "Doc 4 (actual binary without .json extension) should remain BINARY - transform not applied");
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
