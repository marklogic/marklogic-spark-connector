/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.TextDocumentManager;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class WriteDocumentFilesTest extends AbstractIntegrationTest {

    @Test
    void writeFifteenAuthorFiles(@TempDir Path tempDir) throws Exception {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyAuthorFilesWereCorrectlyWritten(tempDir);
    }

    @ParameterizedTest
    @ValueSource(strings = {"doesntexist", "has space", "has+plus"})
    void pathDoesntExist(String directoryName, @TempDir Path tempDir) {
        File dir = new File(tempDir.toFile(), directoryName);
        assertFalse(dir.exists());

        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(dir.getAbsolutePath());

        assertTrue(dir.exists(), "Directory was not created: " + dir.getAbsolutePath());
        assertEquals(1, dir.listFiles().length);
        assertTrue(new File(dir, "author").exists());
    }

    @Test
    void streamAuthorDocuments(@TempDir Path tempDir) throws Exception {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load();

        assertEquals(15, dataset.count());

        dataset.collectAsList().forEach(row -> assertTrue(row.isNullAt(1),
            "When the 'stream files' option is used when reading documents, the 'content' column should be null " +
                "for each row. When each row is written to a file, the document corresponding to the URI in the " +
                "'uri' column should be retrieved and streamed to file, thus avoiding ever reading the entire " +
                "document into memory."));

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyAuthorFilesWereCorrectlyWritten(tempDir);
    }

    @Test
    void variousURIs(@TempDir Path tempDir) throws Exception {
        DocumentMetadataHandle metadata = TestUtil.withDefaultPermissions(
            new DocumentMetadataHandle().withCollections("uri-examples"));
        TextDocumentManager mgr = getDatabaseClient().newTextDocumentManager();
        DocumentWriteSet writeSet = mgr.newWriteSet();
        writeSet.add("example.txt", metadata, new StringHandle("URI without leading slash"));
        writeSet.add("org:example2.txt", metadata, new StringHandle("Opaque URI"));
        mgr.write(writeSet);

        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "uri-examples")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File[] files = tempDir.toFile().listFiles();
        List<String> filenames = Arrays.stream(files).map(f -> f.getName()).collect(Collectors.toList());
        assertEquals(2, filenames.size());
        assertTrue(filenames.contains("example.txt"), "Unexpected filenames: " + filenames);
        assertTrue(filenames.contains("example2.txt"), "MLCP has a check for an 'opaque' URI, of which org:example2.txt " +
            "is an example. An opaque URI does not have a path, so its 'scheme-specific' part is expected to be used " +
            "as the filename; actual filenames: " + filenames);

        String content = new String(FileCopyUtils.copyToByteArray(files[filenames.indexOf("example.txt")]));
        assertEquals("URI without leading slash", content);
        content = new String(FileCopyUtils.copyToByteArray(files[filenames.indexOf("example2.txt")]));
        assertEquals("Opaque URI", content);
    }

    @Test
    void uriHasSpace(@TempDir Path tempDir) {
        final String uri = "/has space.json";

        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/spark-json/single-object.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "char-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, uri)
            .mode(SaveMode.Append)
            .save();

        sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File dir = tempDir.toFile();
        assertEquals(1, dir.listFiles().length);
        String filename = dir.listFiles()[0].getName();
        assertEquals("has space.json", filename,
            "Just like MLCP, if the connector cannot construct a java.net.URI from the document URI (it will fail " +
                "due to a space), the error should be logged and the file should be written with its unaltered " +
                "document URI used for the file path.");
    }

    private void verifyAuthorFilesWereCorrectlyWritten(Path tempDir) throws Exception {
        for (int i = 1; i <= 15; i++) {
            File expectedFile = Paths.get(
                tempDir.toFile().getAbsolutePath(),
                "author", "author" + i + ".json"
            ).toFile();
            assertTrue(expectedFile.exists(), "Expected file at: " + expectedFile);

            // Verify the JSON is valid.
            JsonNode doc = objectMapper.readTree(expectedFile);
            assertTrue(doc.has("CitationID"));
            assertTrue(doc.has("LastName"));
        }
    }
}
