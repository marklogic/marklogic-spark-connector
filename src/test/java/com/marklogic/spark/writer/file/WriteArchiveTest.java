package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteArchiveTest extends AbstractIntegrationTest {

    @Test
    void writeAllMetadata(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("metadata")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "metadata");
    }

    @Test
    void writePermissions(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("permissions")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "permissions");
    }

    @Test
    void writeCollections(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("collections")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "collections");
    }

    @Test
    void writeQuality(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("quality")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "quality");
    }

    @Test
    void writeProperties(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("properties")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "");
    }

    @Test
    void writeMetadataValues(@TempDir Path tempDir) throws Exception {
        readAuthorCollectionWithMetadata("metadatavalues")
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);

        verifyZipFilesHaveExpectedFilenames(tempDir);
        verifyMetadataFiles(tempDir, "");
    }

    private Dataset<Row> readAuthorCollectionWithMetadata(String metadataValue) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,"+metadataValue)
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load();
    }

    private void verifyZipFilesHaveExpectedFilenames(@TempDir Path tempDir) {
        final String expectedPrefix = new SimpleDateFormat("yyyyMMddHH").format(new Date());
        for (File file : tempDir.toFile().listFiles()) {
            String name = file.getName();
            assertTrue(name.startsWith(expectedPrefix), String.format("Filename %s did not start with %s", name, expectedPrefix));
            assertTrue(name.endsWith(".zip"));
        }
    }

    private void verifyMetadataFiles(Path tempDir, String metadataValue) throws IOException, InvocationTargetException, IllegalAccessException {
        final List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        assertEquals(30, rows.size());

        final String expectedUriPrefix = "file://" + tempDir.toFile().getAbsolutePath();
        for (Row row : rows) {
            String uri = row.getString(0);
            assertTrue(uri.startsWith(expectedUriPrefix), "Unexpected URI, which is expected to start with the " +
                "absolute path of the zip file: " + uri);

            if(uri.endsWith(".json")) {
                JsonNode doc = objectMapper.readTree((byte[]) row.get(3));
                assertTrue(doc.has("CitationID"), "Unexpected JSON: " + doc);
            } else {
                assertTrue(uri.endsWith(".metadata"));
                switch (metadataValue){
                    case "collections":
                        verifyCollections(row);
                        break;
                    case "permissions":
                        verifyPermissions(row);
                        break;
                    case "quality":
                        verifyQuality(row);
                        break;
                    case "metadata":
                        verifyCollections(row);
                        verifyPermissions(row);
                        verifyQuality(row);
                        break;
                }
            }
        }
    }

    private void verifyCollections(Row row) {
        String metadataContent = new String((byte[]) row.get(3), StandardCharsets.UTF_8);
        XmlNode metadata = new XmlNode(metadataContent, Namespace.getNamespace("rapi", "http://marklogic.com/rest-api"));
        metadata.assertElementValue("/rapi:metadata/rapi:collections/rapi:collection", "author");
    }

    private void verifyPermissions(Row row) {
        String metadataContent = new String((byte[]) row.get(3), StandardCharsets.UTF_8);
        XmlNode metadata = new XmlNode(metadataContent, Namespace.getNamespace("rapi", "http://marklogic.com/rest-api"));
        metadata.assertElementValue("/rapi:metadata/rapi:permissions/rapi:permission/rapi:role-name", "rest-writer");
        metadata.assertElementValue("/rapi:metadata/rapi:permissions/rapi:permission/rapi:capability", "update");
    }

    private void verifyQuality(Row row) {
        String metadataContent = new String((byte[]) row.get(3), StandardCharsets.UTF_8);
        XmlNode metadata = new XmlNode(metadataContent, Namespace.getNamespace("rapi", "http://marklogic.com/rest-api"));
        metadata.assertElementValue("/rapi:metadata/rapi:quality", "0");
    }
}
