package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        System.out.println(filename);
        assertEquals("has space.json", filename,
            "Just like MLCP, if the connector cannot construct a java.net.URI from the document URI (it will fail " +
                "due to a space), the error should be logged and the file should be written with its unaltered " +
                "document URI used for the file path.");
    }
}
