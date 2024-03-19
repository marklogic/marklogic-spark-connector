package com.marklogic.spark.reader.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadMlcpArchiveFilesTest extends AbstractIntegrationTest {

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
    void mlcpArchivesContainingAllFourTypesOfDocuments() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/all-four-document-types")
            .sort(new Column("URI"))
            .collectAsList();

        assertEquals(4, rows.size());

        assertEquals("JSON", rows.get(0).getString(2), "MLCP appears to have a bug where a binary file has its " +
            "format captured as JSON. MLE-12923 was created to capture this bug. Once the bug is fixed, this should " +
            "equal BINARY.");
        assertEquals("JSON", rows.get(1).getString(2));
        assertEquals("TEXT", rows.get(2).getString(2));
        assertEquals("XML", rows.get(3).getString(2));
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
        final String uri = row.getString(0);
        assertTrue(uri.endsWith("/files-with-all-metadata.mlcp.zip/test/1.xml"), "Unexpected URI: " + uri);

        XmlNode doc = new XmlNode(new String((byte[]) row.get(1)));
        assertEquals("world", doc.getElementValue("hello"));

        verifyRowMetadata(row);
    }

    private void verifySecondRow(Row row) {
        final String uri = row.getString(0);
        assertTrue(uri.endsWith("/files-with-all-metadata.mlcp.zip/test/2.xml"), "Unexpected URI: " + uri);

        XmlNode doc = new XmlNode(new String((byte[]) row.get(1)));
        assertEquals("world", doc.getElementValue("hello"));

        verifyRowMetadata(row);
    }

    private void verifyRowMetadata(Row row) {
        assertEquals("XML", row.getString(2)); // format

        List<String> collections = row.getList(3);
        assertEquals(2, collections.size());
        assertEquals("collection1", collections.get(0));
        assertEquals("collection2", collections.get(1));

        Map<String, WrappedArray<String>> permissions = row.getJavaMap(4);
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

        assertEquals(10, row.getInt(5)); // quality

        Map<String, String> properties = row.getJavaMap(6);
        assertEquals(2, properties.size());
        assertEquals("value2", properties.get("key2"));
        assertEquals("value1", properties.get("{org:example}key1"));

        Map<String, String> metadataValues = row.getJavaMap(7);
        assertEquals(2, metadataValues.size());
        assertEquals("value1", metadataValues.get("meta1"));
        assertEquals("value2", metadataValues.get("meta2"));
    }
}
