/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
        newWriter(2).save();

        // Just verifies that the operation succeeds with multiple partitions. Check the logging to see that two
        // partitions were in fact created, each with its own WriteBatcher.
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void temporalTest() {
        newWriterWithDefaultConfig("temporal-data.csv", 1)
            .option(Options.WRITE_TEMPORAL_COLLECTION, TEMPORAL_COLLECTION)
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        // Temporal doc is written to the temporal collection; "latest" since it's the latest version for that URI;
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
        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
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
        ConnectorException ex = assertThrowsConnectorException(() -> newWriter()
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
        ConnectorException ex = assertThrowsConnectorException(() -> newWriter()
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

    private void verifyFailureIsDueToLackOfPermission(ConnectorException ex) {
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
