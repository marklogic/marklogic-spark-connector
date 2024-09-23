/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadMlcpArchiveFilesTest extends AbstractIntegrationTest {

    private static final int COLLECTIONS_COLUMN = 3;
    private static final int PERMISSIONS_COLUMN = 4;
    private static final int QUALITY_COLUMN = 5;
    private static final int PROPERTIES_COLUMN = 6;
    private static final int METADATAVALUES_COLUMN = 7;

    /**
     * Depends on an MLCP archive file containing the two XML documents and their metadata entries as produced by
     * TestUtil.insertTwoDocumentsWithAllMetadata.
     */
    @Test
    void readMlcpArchiveFile() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/files-with-all-metadata.mlcp.zip")
            .collectAsList();

        assertEquals(2, rows.size(), "The .metadata entries should not be returned as rows, but rather should be " +
            "used to populate the metadata columns in the rows for the documents they're associated with.");

        verifyFirstRow(rows.get(0));
        verifySecondRow(rows.get(1));
    }

    @Test
    void twoArchivesOnePartition() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load(
                "src/test/resources/mlcp-archive-files/complex-properties.zip",
                "src/test/resources/mlcp-archive-files/files-with-all-metadata.mlcp.zip"
            )
            .collectAsList();

        assertEquals(3, rows.size(), "A single partition reader should iterate over the two files and get " +
            "2 rows from files-with-all-metadata and 1 row from complex-properties.");
    }

    @Test
    void mlcpArchivesContainingAllFourTypesOfDocuments() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/all-four-document-types")
            .sort(new Column("URI"))
            .collectAsList();

        assertEquals(4, rows.size());

        rows.forEach(row -> System.out.println(row.prettyJson()));

        assertEquals("/mixed-files/hello.json", rows.get(0).getString(0));
        assertEquals("JSON", rows.get(0).getString(2));

        assertEquals("/mixed-files/hello.txt", rows.get(1).getString(0));
        assertEquals("TEXT", rows.get(1).getString(2));

        assertEquals("/mixed-files/hello.xml", rows.get(2).getString(0));
        assertEquals("XML", rows.get(2).getString(2));

        assertEquals("/mixed-files/hello2.txt.gz", rows.get(3).getString(0));
        assertEquals("JSON", rows.get(3).getString(2), "MLCP appears to have a bug where a binary file has its " +
            "format captured as JSON. MLE-12923 was created to capture this bug. Once the bug is fixed, this should " +
            "equal BINARY.");
    }

    @Test
    void subsetOfCategories() {
        readArchiveWithCategories("collections,permissions,quality").forEach(row -> {
            verifyCollections(row);
            verifyPermissions(row);
            verifyQuality(row);
            verifyColumnsAreNull(row, PROPERTIES_COLUMN, METADATAVALUES_COLUMN);
        });
    }

    @Test
    void onlyCollections() {
        readArchiveWithCategories("collections").forEach(row -> {
            verifyCollections(row);
            verifyColumnsAreNull(row, PERMISSIONS_COLUMN, QUALITY_COLUMN, PROPERTIES_COLUMN, METADATAVALUES_COLUMN);
        });
    }

    @Test
    void onlyPermissions() {
        readArchiveWithCategories("permissions").forEach(row -> {
            verifyPermissions(row);
            verifyColumnsAreNull(row, COLLECTIONS_COLUMN, QUALITY_COLUMN, PROPERTIES_COLUMN, METADATAVALUES_COLUMN);
        });
    }

    @Test
    void onlyQuality() {
        readArchiveWithCategories("quality").forEach(row -> {
            verifyQuality(row);
            verifyColumnsAreNull(row, COLLECTIONS_COLUMN, PERMISSIONS_COLUMN, PROPERTIES_COLUMN, METADATAVALUES_COLUMN);
        });
    }

    @Test
    void onlyProperties() {
        readArchiveWithCategories("properties").forEach(row -> {
            verifyProperties(row);
            verifyColumnsAreNull(row, COLLECTIONS_COLUMN, PERMISSIONS_COLUMN, QUALITY_COLUMN, METADATAVALUES_COLUMN);
        });
    }

    @Test
    void onlyMetadataValues() {
        readArchiveWithCategories("metadatavalues").forEach(row -> {
            verifyMetadataValues(row);
            verifyColumnsAreNull(row, COLLECTIONS_COLUMN, PERMISSIONS_COLUMN, QUALITY_COLUMN, PROPERTIES_COLUMN);
        });
    }

    @Test
    void complexProperties() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/complex-properties.zip")
            .collectAsList();

        assertEquals(1, rows.size());

        XmlNode properties = new XmlNode(rows.get(0).getString(PROPERTIES_COLUMN),
            PROPERTIES_NAMESPACE, Namespace.getNamespace("flexrep", "http://marklogic.com/xdmp/flexible-replication"));
        properties.assertElementValue(
            "This verifies that the properties column can contain any serialized string of XML. This is necessary so " +
                "that complex XML structures can be read from and written to MarkLogic.",
            "/prop:properties/flexrep:document-status/flexrep:document-uri", "/equipment/DX06040.json");
    }

    @Test
    void notAnMlcpArchive() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/zip-files/mixed-files.zip");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to read metadata for entry: mixed-files/hello.json"), "Unexpected message: " + message);
    }

    @Test
    void notAZipFile() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/logback.xml");

        assertEquals(0, dataset.count(), "Just like with reading zip files, the underlying Java ZipInputStream " +
            "does not throw an error when it reads a non-zip file. It just doesn't return any zip entries. For now, " +
            "we are not treating this as an error condition either. The user will simply get back zero rows.");
    }

    @Test
    void dontAbortOnBadArchiveFile() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load(
                "src/test/resources/zip-files/mixed-files.zip",
                "src/test/resources/mlcp-archive-files/files-with-all-metadata.mlcp.zip"
            )
            .collectAsList();

        assertEquals(2, rows.size(), "When abortOnFailure is false, the error from mixed-files.zip should be logged " +
            "and the connector should keep processing other files, thus returning the 2 valid rows from " +
            "files-with-all-metadata.mlcp.zip.");
    }

    @Test
    void dontAbortOnArchiveFileMissingContentEntry() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load(
                "src/test/resources/mlcp-archive-files/missing-content-entry.mlcp.zip",
                "src/test/resources/mlcp-archive-files/files-with-all-metadata.mlcp.zip"
            )
            .collectAsList();

        assertEquals(3, rows.size(), "The connector should get 1 valid row from missing-content-entry.mlcp.zip, " +
            "as it's the second metadata entry that is missing a content entry. It should also get 2 rows from " +
            "files-with-all-metadata.mlcp.zip. And the error from the missing content entry should be logged but " +
            "not thrown.");
    }

    private void verifyFirstRow(Row row) {
        assertEquals("/test/1.xml", row.getString(0));
        XmlNode doc = new XmlNode(new String((byte[]) row.get(1)));
        assertEquals("world", doc.getElementValue("hello"));
        verifyRowMetadata(row);
    }

    private void verifySecondRow(Row row) {
        assertEquals("/test/2.xml", row.getString(0));
        XmlNode doc = new XmlNode(new String((byte[]) row.get(1)));
        assertEquals("world", doc.getElementValue("hello"));
        verifyRowMetadata(row);
    }

    private void verifyRowMetadata(Row row) {
        assertEquals("XML", row.getString(2)); // format
        verifyCollections(row);
        verifyPermissions(row);
        verifyQuality(row);
        verifyProperties(row);
        verifyMetadataValues(row);
    }

    private List<Row> readArchiveWithCategories(String categories) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .option(Options.READ_ARCHIVES_CATEGORIES, categories)
            .load("src/test/resources/mlcp-archive-files/files-with-all-metadata.mlcp.zip")
            .collectAsList();
    }

    private void verifyCollections(Row row) {
        List<String> collections = row.getList(COLLECTIONS_COLUMN);
        assertEquals(2, collections.size());
        assertEquals("collection1", collections.get(0));
        assertEquals("collection2", collections.get(1));
    }

    private void verifyPermissions(Row row) {
        Map<String, WrappedArray<String>> permissions = row.getJavaMap(PERMISSIONS_COLUMN);
        assertEquals(2, permissions.size());
        WrappedArray<String> capabilities = permissions.get("qconsole-user");
        assertEquals(1, capabilities.size());
        assertEquals("READ", capabilities.apply(0));
        capabilities = permissions.get("spark-user-role");
        assertEquals(2, capabilities.size());
        List<String> list = new ArrayList<>();
        list.add(capabilities.apply(0));
        list.add(capabilities.apply(1));
        assertTrue(list.contains("READ"));
        assertTrue(list.contains("UPDATE"));
    }

    private void verifyQuality(Row row) {
        assertEquals(10, row.getInt(QUALITY_COLUMN));
    }

    private void verifyProperties(Row row) {
        XmlNode properties = new XmlNode(row.getString(PROPERTIES_COLUMN),
            PROPERTIES_NAMESPACE, Namespace.getNamespace("ex", "org:example"));
        properties.assertElementValue("/prop:properties/ex:key1", "value1");
        properties.assertElementValue("/prop:properties/key2", "value2");
    }

    private void verifyMetadataValues(Row row) {
        Map<String, String> metadataValues = row.getJavaMap(METADATAVALUES_COLUMN);
        assertEquals(2, metadataValues.size());
        assertEquals("value1", metadataValues.get("meta1"));
        assertEquals("value2", metadataValues.get("meta2"));
    }

    private void verifyColumnsAreNull(Row row, int... indices) {
        for (int index : indices) {
            assertTrue(row.isNullAt(index), "Unexpected non-null column: " + index + "; value: " + row.get(index));
        }
    }
}
