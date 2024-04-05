package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteArchiveOfFailedDocumentsTest extends AbstractWriteTest {

    @Test
    void test(@TempDir Path tempDir) {
        SparkSession session = newSparkSession();

        defaultWrite(session.read().format("binaryFile")
            .load("src/test/resources/mixed-files")
            .repartition(1) // Forces a single partition writer and thus a single archive file being written.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS, tempDir.toFile().getAbsolutePath())
        );

        assertCollectionSize("Only the JSON document should have succeeded; error messages should have been logged " +
            "for the other 3 documents.", "partial-batch", 1);

        // Read the archive file back in and verify the contents.
        List<Row> rows = session.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toFile().getAbsolutePath())
            .sort(new Column("URI"))
            .collectAsList();
        verifyArchiveRows(rows);
    }

    @Test
    void invalidArchivePath() {
        ConnectorException ex = assertThrowsConnectorException(() -> defaultWrite(newSparkSession().read().format("binaryFile")
            .load("src/test/resources/mixed-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_COLLECTIONS, "should-be-empty")
            .option(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS, "/invalid/path/doesnt/exist")
        ));

        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to create archive file for failed documents at path /invalid/path/doesnt/exist; cause"),
            "An invalid path or path that cannot be written to is expected to cause an immediate failure before anything " +
                "is written; unexpected error: " + message);

        assertCollectionSize("should-be-empty", 0);
    }

    @Test
    void multipleFailedBatches(@TempDir Path tempDir) {
        newSparkSession().read().format("json")
            .option("multiLine", true)
            .load("src/test/resources/500-employees.json")
            .repartition(4)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_SUFFIX, ".xml")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS, tempDir.toFile().getAbsolutePath())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("partial-batch", 0);
        assertEquals(4, tempDir.toFile().listFiles().length, "Expecting 1 archive for each of the 4 partition writers.");

        long count = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toFile().getAbsolutePath())
            .count();

        assertEquals(500, count,
            "All 500 employee docs should have failed since we tried to write them as XML documents. All 500 " +
                "should have been written to 4 separate archive files.");
    }

    private void verifyArchiveRows(List<Row> rows) {
        assertEquals(3, rows.size(), "Expecting one row for each of the 3 failed documents.");

        Row row = rows.get(0);
        assertTrue(row.getString(0).endsWith("/hello.txt.json"), "Unexpected URI: " + row.getString(0));
        verifyMetadataColumns(row);

        row = rows.get(1);
        assertTrue(row.getString(0).endsWith("/hello.xml.json"), "Unexpected URI: " + row.getString(0));
        verifyMetadataColumns(row);

        row = rows.get(2);
        assertTrue(row.getString(0).endsWith("/hello2.txt.gz.json"), "Unexpected URI: " + row.getString(0));
        verifyMetadataColumns(row);
    }

    private void verifyMetadataColumns(Row row) {
        List<String> collections = JavaConversions.seqAsJavaList(row.getSeq(3));
        assertEquals(1, collections.size());
        assertEquals("partial-batch", collections.get(0));

        Map<String, WrappedArray> permissions = row.getJavaMap(4);
        assertTrue(permissions.get("spark-user-role").toString().contains("READ"));
        assertTrue(permissions.get("spark-user-role").toString().contains("UPDATE"));

        assertEquals(0, row.getInt(5));

        Map<String, WrappedArray> properties = row.getJavaMap(6);
        assertEquals(0, properties.size());

        Map<String, WrappedArray> metadataValues = row.getJavaMap(7);
        assertEquals(0, metadataValues.size());
    }
}
