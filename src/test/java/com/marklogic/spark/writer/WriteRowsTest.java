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
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class WriteRowsTest extends AbstractWriteTest {

    @Test
    void defaultBatchSizeAndThreadCount() {
        newWriter().save();
        verifyTwoHundredDocsWereWritten();
    }

    @Test
    void batchSizeGreaterThanNumberOfRowsToWrite() {
        newWriter()
            .option(Options.WRITE_BATCH_SIZE, 1000)
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
        String temporalCollection = "temporal-collection";
        newWriterWithDefaultConfig("temporal-data.csv", 1)
            .option(Options.WRITE_TEMPORAL_COLLECTION, temporalCollection)
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        // Temporal doc is written to the temporal collection; "latest" since it's the latest version for that URI;
        // and to a collection matching the URI of the document.
        assertInCollections(uri, COLLECTION, temporalCollection, "latest", uri);
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
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriter().option(Options.WRITE_THREAD_COUNT, 0).save()
        );

        assertTrue(ex.getCause() instanceof IllegalArgumentException, "Unexpected cause: " + ex.getCause().getClass());
        assertEquals("Value of 'spark.marklogic.write.threadCount' option must be 1 or greater", ex.getCause().getMessage());
        verifyNoDocsWereWritten();
    }

    @Test
    void invalidBatchSize() {
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriter().option(Options.WRITE_BATCH_SIZE, 0).save()
        );

        assertTrue(ex.getCause() instanceof IllegalArgumentException, "Unexpected cause: " + ex.getCause().getClass());
        assertEquals("Value of 'spark.marklogic.write.batchSize' option must be 1 or greater", ex.getCause().getMessage(),
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
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriter()
                .option("spark.marklogic.client.username", "spark-no-write-user")
                .option(Options.WRITE_BATCH_SIZE, 500)
                .save()
        );

        verifyFailureIsDueToLackOfPermission(ex);
    }

    /**
     * Uses a batch size of 1 to ensure that the write() method in the data writer should fail, as each write() call
     * should result in a request to MarkLogic, which should cause a failure before commit() is called.
     */
    @Test
    void userNotPermittedToWriteAndFailOnWrite() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriter()
                .option("spark.marklogic.client.username", "spark-no-write-user")
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

        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertEquals("Unable to parse permissions string, which must be a comma-separated list of role names and " +
            "capabilities - i.e. role1,read,role2,update,role3,execute; " +
            "string: rest-reader,read,rest-writer", ex.getCause().getMessage());
    }

    private void verifyFailureIsDueToLackOfPermission(SparkException ex) {
        assertTrue(ex.getCause() instanceof IOException, "Unexpected cause: " + ex.getCause().getClass());
        assertTrue(ex.getCause().getMessage().contains("Server Message: You do not have permission to this method and URL"),
            "Unexpected cause message: " + ex.getCause().getMessage());
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
