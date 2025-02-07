/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReadArchiveFileTest extends AbstractIntegrationTest {

    @BeforeEach
    void beforeEach() {
        TestUtil.insertTwoDocumentsWithAllMetadata(getDatabaseClient());
    }

    @Test
    void testAllMetadata(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyAllMetadata(tempDir, 2);
    }

    @Test
    void testCollectionsAndPermissions(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_ARCHIVES_CATEGORIES, "collections,permissions")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(2, rows.size(), "Expecting 2 rows in the zip.");
        rows.forEach(row -> {
            verifyContent(row);
            assertTrue(row.isNullAt(2));
            verifyCollections(row);
            verifyPermissions(row);
            assertNull(row.get(5), "Quality column should be null.");
            assertNull(row.get(6), "Properties column should be null.");
            assertNull(row.get(7), "MetadataValues column should be null.");
        });
    }

    @Test
    void testQualityPropertiesAndMetadatavalues(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_ARCHIVES_CATEGORIES, "quality,properties,metadataValues")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(2, rows.size(), "Expecting 2 rows in the zip.");
        rows.forEach(row -> {
            verifyContent(row);
            assertTrue(row.isNullAt(2));
            assertNull(row.get(3), "Collections column should be null.");
            assertNull(row.get(4), "Permissions column should be null.");
            assertEquals(10, row.get(5));
            verifyProperties(row);
            verifyMetadatavalues(row);
        });
    }

    @Test
    void testCollectionsAndMetadataValues(@TempDir Path tempDir) {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_ARCHIVES_CATEGORIES, "collections,metadataValues")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(2, rows.size(), "Expecting 2 rows in the zip.");
        rows.forEach(row -> {
            verifyContent(row);
            assertTrue(row.isNullAt(2));
            verifyCollections(row);
            assertNull(row.get(4), "Permissions column should be null.");
            assertNull(row.get(5), "Quality column should be null.");
            assertNull(row.get(6), "Properties column should be null.");
            verifyMetadatavalues(row);
        });
    }

    @Test
    void invalidArchiveAndAbort() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load("src/test/resources/archive-files/firstEntryInvalid.zip");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        assertTrue(ex.getMessage().startsWith("Could not find metadata entry for entry test/1.xml in file"),
            "The connector should default to throwing an error when it cannot find a metadata entry; error: " + ex.getMessage());
    }

    @Test
    void invalidArchiveDontAbort() {
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load(
                "src/test/resources/archive-files/firstEntryInvalid.zip",
                "src/test/resources/archive-files/archive1.zip"
            )
            .collectAsList();

        assertEquals(2, rows.size(), "Because the first entry in bad.zip is invalid as it does not have a metadata " +
            "entry, no rows should be returned for that zip, but the connector should still process the valid " +
            "zip and return its 2 rows.");

        assertEquals("/test/1.xml", rows.get(0).getString(0));
        assertEquals("/test/2.xml", rows.get(1).getString(0));
    }

    @Test
    void secondEntryInvalidDontAbort() {
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/archive-files/secondEntryInvalid.zip")
            .collectAsList();

        assertEquals(1, rows.size(), "The first entry in the zip is valid, and so a row should be returned for it " +
            "and its associated metadata entry. The second entry is invalid because it's missing a metadata entry. " +
            "But no error should be thrown since the connector is configured to not abort on failure.");
        assertEquals("test/1.xml", rows.get(0).getString(0));
    }

    @Test
    void threeZipsOnePartition() {
        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load(
                "src/test/resources/archive-files/archive1.zip",
                "src/test/resources/archive-files/firstEntryInvalid.zip",
                "src/test/resources/archive-files/secondEntryInvalid.zip"
            )
            .collectAsList();

        assertEquals(3, rows.size(), "Expecting 2 rows from archive1.zip, none from firstEntryInvalid.zip, " +
            "and 1 from secondEntryInvalid.zip.");
    }

    /**
     * Verifies that the encoding is applied to documents read from an archive zip. In this case, iso-8859-1 is required
     * for the content entry but still works fine for the metadata entry, which is UTF-8.
     */
    @Test
    void customEncoding() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_FILES_ENCODING, "iso-8859-1")
            .load("src/test/resources/encoding/medline.iso-8859-1.archive.zip")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("test/medline.iso-8859-1.xml", "collection1");
        doc.assertElementExists("/MedlineCitationSet");
    }

    @Test
    void pathDoesntExist() {
        AnalysisException ex = assertThrows(AnalysisException.class, () -> newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load("path-doesnt-exist"));

        assertTrue(ex.getMessage().contains("Path does not exist"), "Unexpected error: " + ex.getMessage());
    }

    private void verifyAllMetadata(Path tempDir, int rowCount) {
        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(rowCount, rows.size(), "Expecting 2 rows in the zip.");

        for (int i = 0; i < rowCount; i++) {
            Row row = rows.get(i);
            assertTrue(row.getString(0).endsWith("/test/" + (i + 1) + ".xml"));
            verifyContent(row);
            assertTrue(row.isNullAt(2), "There's no indication in an archive file as to what the format of a " +
                "content entry is, so the 'format' column should always be null.");
            verifyCollections(row);
            verifyPermissions(row);
            assertEquals(10, row.get(5));
            verifyProperties(row);
            verifyMetadatavalues(row);
        }
    }

    private void verifyContent(Row row) {
        String content = new String((byte[]) row.get(1), StandardCharsets.UTF_8);
        assertTrue(content.contains("<hello>world</hello>"));
    }

    private void verifyCollections(Row row) {
        List<String> collections = JavaConversions.seqAsJavaList(row.getSeq(3));
        assertEquals("collection1", collections.get(0));
        assertEquals("collection2", collections.get(1));
    }

    private void verifyPermissions(Row row) {
        Map<String, WrappedArray> permissions = row.getJavaMap(4);
        assertTrue(permissions.get("spark-user-role").toString().contains("READ"));
        assertTrue(permissions.get("spark-user-role").toString().contains("UPDATE"));
        assertTrue(permissions.get("qconsole-user").toString().contains("READ"));
    }

    private void verifyProperties(Row row) {
        XmlNode properties = new XmlNode(row.getString(6), PROPERTIES_NAMESPACE, Namespace.getNamespace("ex", "org:example"));
        properties.assertElementValue("/prop:properties/ex:key1", "value1");
        properties.assertElementValue("/prop:properties/key2", "value2");
    }

    private void verifyMetadatavalues(Row row) {
        Map<String, String> metadataValues = row.getJavaMap(7);
        assertEquals("value1", metadataValues.get("meta1"));
        assertEquals("value2", metadataValues.get("meta2"));
    }
}
