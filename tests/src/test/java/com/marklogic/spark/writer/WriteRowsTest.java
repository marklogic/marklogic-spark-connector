/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


class WriteRowsTest extends AbstractWriteTest {

    private static final String TEMPORAL_COLLECTION = "temporal-collection";

    @Test
    void defaultBatchSizeAndThreadCount() {
        newWriter().save();
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void logProgressTest() {
        newWriter(4)
            // Including these options here to ensure they don't cause any issues, though we're not yet able to
            // assert on the info-level log entries that they add.
            .option(Options.WRITE_BATCH_SIZE, 8)
            .option(Options.WRITE_THREAD_COUNT, 8)
            .option(Options.WRITE_LOG_PROGRESS, 20)
            .save();

        verifyTwoHundredDocsWereWritten();

        // For manual inspection, run it again to ensure that the progress counter was reset.
        newWriter(2)
            .option(Options.WRITE_BATCH_SIZE, 10)
            .option(Options.WRITE_LOG_PROGRESS, 40)
            .save();
    }

    @Test
    void batchSizeGreaterThanNumberOfRowsToWrite() {
        newWriter()
            .option(Options.WRITE_BATCH_SIZE, 1000)
            // Adding this here just to ensure an error doesn't happen. While it doesn't make sense for a user to apply
            // this option when there's no initial URI, it doesn't cause any issues and thus there's no need to throw
            // an error.
            .option(Options.WRITE_URI_REPLACE, "thiswont,'have any effect'")
            .save();

        // Verifies that the docs were written during the "commit()" call, as the WriteBatcher is expected to be
        // flushed during that call.
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void twoPartitions() {
        newWriter(2)
            .option(Options.WRITE_THREAD_COUNT_PER_PARTITION, 8)
            .option(Options.WRITE_BATCH_SIZE, 10)
            .save();

        // Just verifies that the operation succeeds with multiple partitions. Check the logging to see that two
        // partitions were in fact created, each with its own WriteBatcher.
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void insufficientPrivilegeForOtherDatabase() {
        DataFrameWriter writer = newWriter(2)
            .option(Options.WRITE_THREAD_COUNT_PER_PARTITION, 8)
            .option(Options.WRITE_BATCH_SIZE, 10)
            .option(Options.CLIENT_URI, String.format("spark-test-user:spark@%s:%d/Documents",
                testConfig.getHost(), testConfig.getRestPort()));

        SparkException ex = assertThrows(SparkException.class, () -> writer.save());
        assertNull(ex.getCause(), "Surprisingly, in this scenario where the exception is thrown during the " +
            "construction of WriteBatcherDataWriter, Spark does not populate the 'cause' of the exception but rather " +
            "shoves the entire stacktrace of the exception into the exception message. This is not a good UX for " +
            "connector or Flux users, as it puts an ugly stacktrace right into their face. I have not figured out " +
            "how to avoid this yet, so this test is capturing this behavior in the hopes that an upgraded version of " +
            "Spark will properly set the cause instead.");
        assertTrue(ex.getMessage().contains("at com.marklogic.client.impl.OkHttpServices"), "This is confirming that " +
            "the exception message contains the stacktrace of the MarkLogic exception - which we don't want. Hoping " +
            "this assertion breaks during a future upgrade of Spark and we have a proper exception message " +
            "instead. Actual message: " + ex.getMessage());
    }

    @Test
    void temporalTest() {
        newWriterWithDefaultConfig("temporal-data.csv", 1)
            .option(Options.WRITE_TEMPORAL_COLLECTION, TEMPORAL_COLLECTION)
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        // Temporal doc is written to the temporal collection and 'latest' since it's the latest version for that URI,
        // and to a collection matching the URI of the document.
        assertInCollections(uri, COLLECTION, TEMPORAL_COLLECTION, "latest", uri);
    }

    @Test
    void testWithCustomConfig() {
        final String collection = "other-collection";

        newWriterForSingleRow()
            .option(Options.WRITE_COLLECTIONS, collection)
            .option(Options.WRITE_PERMISSIONS, "rest-extension-user,read,rest-writer,update")
            .option(Options.WRITE_URI_PREFIX, "/example/")
            .option(Options.WRITE_URI_SUFFIX, ".txt")
            .save();

        String uri = getUrisInCollection(collection, 1).get(0);
        PermissionsTester perms = readDocumentPermissions(uri);
        perms.assertReadPermissionExists("rest-extension-user");
        perms.assertUpdatePermissionExists("rest-writer");

        assertTrue(uri.startsWith("/example/"));
        assertTrue(uri.endsWith(".txt"));
    }

    @Test
    void invalidThreadCount() {
        DataFrameWriter writer = newWriter().option(Options.WRITE_THREAD_COUNT, 0);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> writer.save());
        assertEquals("The value of 'spark.marklogic.write.threadCount' must be 1 or greater.", ex.getMessage());
        verifyNoDocsWereWritten();
    }

    @Test
    void invalidBatchSize() {
        DataFrameWriter writer = newWriter().option(Options.WRITE_BATCH_SIZE, 0);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> writer.save());
        assertEquals("The value of 'spark.marklogic.write.batchSize' must be 1 or greater.", ex.getMessage(),
            "Note that batchSize is very different for writing than it is for reading. For writing, it specifies the " +
                "exact number of documents to send to MarkLogic in each call. For reading, it used to determine how " +
                "many requests will be made by a partition, and zero is a valid value for reading.");
        verifyNoDocsWereWritten();
    }

    /**
     * Verifies that the commit() method in the data writer will fail when it flushes and waits. This is done by
     * using a batch size greater than the number of documents to be written (200) so that nothing is written until the
     * commit() method is invoked.
     */
    @Test
    void userNotPermittedToWriteAndFailOnCommit() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriter()
            .option(Options.CLIENT_USERNAME, "spark-no-write-user")
            .option(Options.WRITE_BATCH_SIZE, 500)
            .save()
        );

        verifyFailureIsDueToLackOfPermission(ex);
    }

    @Test
    void invalidPassword() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriter()
                .option(Options.CLIENT_URI, "spark-test-user:wrong-password@" + testConfig.getHost() + ":" + testConfig.getRestPort())
                .save()
        );

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof RuntimeException, "Expecting a RuntimeException due to the invalid " +
            "credentials; unexpected cause: " + cause);

        assertEquals("Unable to connect to MarkLogic; status code: 401; error message: Unauthorized", cause.getMessage());
    }

    /**
     * Uses a batch size of 1 to ensure that the write() method in the data writer should fail, as each write() call
     * should result in a request to MarkLogic, which should cause a failure before commit() is called.
     */
    @Test
    void userNotPermittedToWriteAndFailOnWrite() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriter()
            .option(Options.CLIENT_USERNAME, "spark-no-write-user")
            .option(Options.WRITE_BATCH_SIZE, 1)
            .option(Options.WRITE_THREAD_COUNT, 1)
            .save()
        );

        verifyFailureIsDueToLackOfPermission(ex);
    }

    @Test
    void invalidPermissionsConfig() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriter()
            .option(Options.WRITE_PERMISSIONS, "rest-reader,read,rest-writer")
            .save());

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof IllegalArgumentException);
        assertEquals("Unable to parse permissions string, which must be a comma-delimited list of role names and " +
            "capabilities - i.e. role1,read,role2,update,role3,execute; " +
            "string: rest-reader,read,rest-writer", cause.getMessage());
    }

    @Test
    void invalidPermissionsCapability() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriter()
            .option(Options.WRITE_PERMISSIONS, "rest-reader,read,rest-writer,notvalid")
            .save());

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof IllegalArgumentException);
        assertEquals("Unable to parse permissions string: rest-reader,read,rest-writer,notvalid; cause: Not a valid capability: NOTVALID",
            cause.getMessage(), "When a capability is invalid, the Java Client throws an error message that refers " +
                "to the enum class. This likely won't make sense to a connector or Flux user. So the connector is " +
                "expected to massage the error a bit by identifying the invalid capability without referencing " +
                "Java classes.");
    }

    @Test
    void dontAbortOnFailure() {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        MarkLogicWrite.setSuccessCountConsumer(count -> successCount.set(count));
        MarkLogicWrite.setFailureCountConsumer(count -> failureCount.set(count));

        newWriterWithDefaultConfig("temporal-data-with-invalid-rows.csv", 1)
            .option(Options.WRITE_TEMPORAL_COLLECTION, TEMPORAL_COLLECTION)
            // Force each row in the CSV to be written in its own batch, ensuring that the one row that should succeed
            // will in fact succeed (if it's stuck in a batch with other bad rows, it'll fail too).
            .option(Options.WRITE_BATCH_SIZE, 1)
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .save();

        assertCollectionSize("9 of the batches should have failed, with the 10th batch succeeding", COLLECTION, 1);
        assertEquals(9, failureCount.get());
        assertEquals(1, successCount.get());
    }

    private void verifyFailureIsDueToLackOfPermission(SparkException ex) {
        assertTrue(ex.getMessage().contains("Server Message: You do not have permission to this method and URL"),
            "Unexpected cause message: " + ex.getMessage());
        verifyNoDocsWereWritten();
    }

    private void verifyTwoHundredDocsWereWritten() {
        final int expectedCollectionSize = 200;
        String uri = getUrisInCollection(COLLECTION, expectedCollectionSize).get(0);
        assertTrue(uri.startsWith("/test/"), "URI should start with '/test/' due to uriPrefix option: " + uri);
        assertTrue(uri.endsWith(".json"), "URI should end with '.json' due to uriSuffix option: " + uri);

        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("docNum"));
        assertTrue(doc.has("docName"));

        PermissionsTester perms = readDocumentPermissions(uri);
        perms.assertReadPermissionExists("spark-user-role");
        perms.assertUpdatePermissionExists("spark-user-role");
    }

    private void verifyNoDocsWereWritten() {
        assertCollectionSize(COLLECTION, 0);
    }
}
