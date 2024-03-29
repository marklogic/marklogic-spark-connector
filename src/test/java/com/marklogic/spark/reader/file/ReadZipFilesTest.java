package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This tests also verifies some of the ways that Spark supports defining a path.
 */
class ReadZipFilesTest extends AbstractIntegrationTest {

    @Test
    void readAndWriteFourFilesInZip() {
        Dataset<Row> reader = newZipReader()
            .load("src/test/resources/zip-files/mixed*.zip");

        verifyFileRows(reader.collectAsList());

        // Now write the rows so we can verify the doc in MarkLogic.
        defaultWrite(reader.write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files.zip,''")
            .option(Options.WRITE_COLLECTIONS, "zip-test")
        );

        assertCollectionSize("zip-test", 4);
        JsonNode doc = readJsonDocument("/mixed-files/hello.json");
        assertEquals("world", doc.get("hello").asText());
        XmlNode xmlDoc = readXmlDocument("/mixed-files/hello.xml");
        assertEquals("world", xmlDoc.getElementValue("/hello"));
        String text = getDatabaseClient().newTextDocumentManager().read("/mixed-files/hello.txt", new StringHandle()).get();
        assertEquals("hello world", text.trim());
        InputStreamHandle handle = getDatabaseClient().newDocumentManager().read("/mixed-files/hello2.txt.gz", new InputStreamHandle());
        assertEquals(Format.BINARY, handle.getFormat());
    }

    @Test
    void readViaMultiplePaths() {
        List<Row> rows = newZipReader()
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(
                "src/test/resources/zip-files/mixed-files.zip",
                "src/test/resources/zip-files/child/logback.zip"
            )
            .collectAsList();

        assertEquals(5, rows.size(), "Expecting 4 rows from mixed-files.zip and 1 row from logback.zip.");
    }

    @Test
    void readTwoZipFilesViaRecursiveLookupWithFilter() {
        List<Row> rows = newZipReader()
            .option("pathGlobFilter", "*.zip")
            .option("recursiveFileLookup", true)
            .load("src/test/resources/zip-files")
            .collectAsList();

        assertEquals(5, rows.size(), "Expecting 4 rows from mixed-files.zip and 1 row from " +
            "logback.zip, which is picked up due to recursiveFileLookup=true.");
    }

    @Test
    void readDirectoryWithNoZips() {
        List<Row> rows = newZipReader()
            .load("src/test/resources/mixed-files")
            .collectAsList();

        assertEquals(0, rows.size(), "Java's ZipInputStream does not throw an error when applied to a file " +
            "that is not a zip. Instead, it does not returning any occurrences of ZipEntry. So no error occurs, " +
            "which is the same behavior as in MLCP.");
    }

    @Test
    void noFilesFoundDueToGlobFilter() {
        List<Row> rows = newZipReader()
            .option("pathGlobFilter", "*.weirdzip")
            .option("recursiveFileLookup", true)
            .load("src/test/resources/zip-files")
            .collectAsList();

        assertEquals(0, rows.size());
    }

    @Test
    void pathDoesntExist() {
        DataFrameReader reader = newZipReader();
        assertThrows(AnalysisException.class, () -> reader.load("path/not/found"), "AnalysisException is a " +
            "standard Spark exception that is thrown when a non-existent path is detected.");
    }

    private void verifyFileRows(List<Row> rows) {
        assertEquals(4, rows.size(), "Expecting 1 row for each of the 4 entries in the zip.");
        Row row = rows.get(0);
        assertTrue(row.getString(0).endsWith("mixed-files.zip/mixed-files/hello.json"));
        assertNull(row.get(1));
        assertEquals(23, row.getLong(2));
        row = rows.get(1);
        assertTrue(row.getString(0).endsWith("mixed-files.zip/mixed-files/hello.txt"));
        assertNull(row.get(1));
        assertEquals(12, row.getLong(2));
        row = rows.get(2);
        assertTrue(row.getString(0).endsWith("mixed-files.zip/mixed-files/hello.xml"));
        assertNull(row.get(1));
        assertEquals(21, row.getLong(2));
        row = rows.get(3);
        assertTrue(row.getString(0).endsWith("mixed-files.zip/mixed-files/hello2.txt.gz"));
        assertNull(row.get(1));
        assertEquals(43, row.getLong(2));
    }

    private DataFrameReader newZipReader() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip");
    }
}
