package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WriteArchiveOfFailedDocumentsTest extends AbstractWriteTest {

    @AfterEach
    void afterEach() {
        MarkLogicWrite.setFailureCountConsumer(null);
        MarkLogicWrite.setSuccessCountConsumer(null);
    }

    @Test
    void happyPath(@TempDir Path tempDir) {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        MarkLogicWrite.setSuccessCountConsumer(count -> successCount.set(count));
        MarkLogicWrite.setFailureCountConsumer(count -> failureCount.set(count));

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
        assertEquals(1, successCount.get());
        assertEquals(3, failureCount.get());

        // Read the archive file back in and verify the contents.
        List<Row> rows = session.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .load(tempDir.toFile().getAbsolutePath())
            .sort(new Column("URI"))
            .collectAsList();
        verifyArchiveRows(rows);
    }

    @Test
    void multiplePartitions(@TempDir Path tempDir) {
        defaultWrite(newSparkSession().read().format("binaryFile")
            .load("src/test/resources/mixed-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_COLLECTIONS, "multiple-partitions")
            .option(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS, tempDir.toFile().getAbsolutePath())
        );

        File[] archiveFiles = tempDir.toFile().listFiles();
        assertEquals(3, archiveFiles.length, "Each file is read and thus written via a separate partition. We " +
            "expect 3 files then, 1 for each failed file. We should not get a 4th file for the partition that was " +
            "able to write a file as a JSON document successfully.");
        assertCollectionSize("multiple-partitions", 1);
    }

    @Test
    void invalidArchivePath() {
        ConnectorException ex = assertThrowsConnectorException(() -> defaultWrite(newSparkSession().read().format("binaryFile")
            .load("src/test/resources/mixed-files")
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_COLLECTIONS, "should-be-empty")
            .option(Options.WRITE_ARCHIVE_PATH_FOR_FAILED_DOCUMENTS, "/invalid/path/doesnt/exist")
        ));

        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to write failed documents to archive file at /invalid/path/doesnt/exist"),
            "The write should have failed because the connector could not write an archive file to the invalid path. " +
                "To avoid creating empty zip files, we don't try to create a zip file right away. The downside of " +
                "this is that we don't 'fail fast' - i.e. we don't know that the path is invalid until a document " +
                "fails to be written, and then we get an error when trying to write that document to an archive " +
                "zip file. However, this scenario seems rare - a user can be expected to typically provide a valid " +
                "path for their archive files. The rarity of that seems acceptable in favor of never creating empty " +
                "zip files, which seems more annoying for a user to deal with. Actual error: " + message);
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

        assertTrue(row.isNullAt(6));

        Map<String, WrappedArray> metadataValues = row.getJavaMap(7);
        assertEquals(0, metadataValues.size());
    }
}
