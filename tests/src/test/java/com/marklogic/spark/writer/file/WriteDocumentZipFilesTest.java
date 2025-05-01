/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.*;

class WriteDocumentZipFilesTest extends AbstractIntegrationTest {

    @Test
    void defaultPartitionCount(@TempDir Path tempDir) throws Exception {
        readAuthorCollection()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(3, tempDir.toFile().listFiles().length, "Expecting 3 zips as the connector should create one " +
            "Spark partition per forest when reading the documents, and each partition should result in a data " +
            "writer that writes to a separate zip file.");

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyZipFilesContainFifteenAuthors(tempDir);
    }

    @Test
    void customPartitionCount(@TempDir Path tempDir) throws IOException {
        readAuthorCollection()
            .repartition(5)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(5, tempDir.toFile().listFiles().length, "Expecting 5 zip files as the Spark repartition() call " +
            "will control how many data writers are created, regardless of how many partition readers were used " +
            "to read the document rows.");

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyZipFilesContainFifteenAuthors(tempDir);
    }

    @Test
    void opaqueURI(@TempDir Path tempDir) throws IOException {
        final String uri = "org:example/123.xml";

        getDatabaseClient().newXMLDocumentManager().write(uri,
            TestUtil.withDefaultPermissions(new DocumentMetadataHandle()).withCollections("opaque-test"),
            new StringHandle("<hello>world</hello>"));

        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "opaque-test")
            .load()
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);
        File file = tempDir.toFile().listFiles()[0];
        ZipFile zipFile = new ZipFile(file);
        assertNotNull(zipFile.getEntry("example/123.xml"), "org:example/123.xml is considered an 'opaque' URI per " +
            "the definition of java.net.URI:isOpaque. Per MLCP behavior, the URI is expected to be set to the " +
            "'schema-specific part', which is just example/123.xml.");
    }

    /**
     * Verifies that streaming documents to a zip file "just works" on account of supporting streaming of archive files
     * first. The same ZipFileWriter is used for both. The only difference with archive files is that it will also
     * check for metadata in each Spark row and include a metadata entry in the archive file.
     *
     * @param tempDir
     * @throws Exception
     */
    @Test
    void streamZipFile(@TempDir Path tempDir) throws Exception {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.STREAM_FILES, true)
            .load();

        assertEquals(15, dataset.count(), "Should have 1 row per author document.");
        dataset.collectAsList().forEach(row -> {
            assertFalse(row.isNullAt(0), "The URI column should be non-null.");
            assertTrue(row.isNullAt(1), "The content column should be empty. The document will be read during the " +
                "writer phase instead.");
        });

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyZipFilesContainFifteenAuthors(tempDir);
    }

    private Dataset<Row> readAuthorCollection() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load();
    }

    private void verifyZipFilesHaveExpectedFilenames(Path tempDir) {
        // Expecting the same prefix that's used by MLCP.
        // Not verifying minutes/seconds as there's a reasonable chance that those can differ slightly between the
        // files being written and then being verified.
        final String expectedPrefix = new SimpleDateFormat("yyyyMMddHH").format(new Date());
        for (File file : tempDir.toFile().listFiles()) {
            String name = file.getName();
            assertTrue(name.startsWith(expectedPrefix), String.format("Filename %s did not start with %s", name, expectedPrefix));
            assertTrue(name.endsWith(".zip"));
        }
    }

    private void verifyZipFilesContainFifteenAuthors(Path tempDir) throws IOException {
        final List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        assertEquals(15, rows.size());

        // Verify each row was read correctly.
        final String expectedUriPrefix = "file:" + tempDir.toFile().getAbsolutePath();
        for (Row row : rows) {
            String uri = row.getString(0);
            assertTrue(uri.startsWith(expectedUriPrefix), "Unexpected URI, which is expected to start with the " +
                "absolute path of the zip file: " + uri);

            JsonNode doc = objectMapper.readTree((byte[]) row.get(1));
            assertTrue(doc.has("CitationID"), "Unexpected JSON: " + doc);
        }
    }
}
