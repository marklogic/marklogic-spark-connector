package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
    void testCollectionsAndMetadatavalues(@TempDir Path tempDir) {
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
        Map<String, String> properties = row.getJavaMap(6);
        assertEquals("value2", properties.get("key2"));
        assertEquals("value1", properties.get("{org:example}key1"));
    }

    private void verifyMetadatavalues(Row row) {
        Map<String, String> metadataValues = row.getJavaMap(7);
        assertEquals("value1", metadataValues.get("meta1"));
        assertEquals("value2", metadataValues.get("meta2"));
    }
}
