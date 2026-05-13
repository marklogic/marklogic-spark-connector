/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import nl.altindag.log.LogCaptor;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IncrementalWriteTest extends AbstractWriteTest {

    @Test
    void defaultSettings() {
        try (LogCaptor logCaptor = LogCaptor.forName(Util.MAIN_LOGGER.getName())) {
            newWriter(1)
                .option(Options.WRITE_INCREMENTAL, true)
                .option(Options.WRITE_BATCH_SIZE, 20)
                .option(Options.WRITE_THREAD_COUNT, 1)
                .option(Options.WRITE_LOG_PROGRESS, 50)
                .option(Options.WRITE_URI_TEMPLATE, "/test/{docNum}.json")
                .option(Options.WRITE_LOG_SKIPPED_DOCUMENTS, 50)
                .save();

            Stream.of(50, 100, 150, 200).forEach(count -> {
                String message = "Documents written: " + count;
                verifyMessageWasLogged(logCaptor, message);
                verifyNoMessageContains(logCaptor, "Documents skipped");
            });

            verifyMessageWasLogged(logCaptor, "Success count: 200");
            verifyNoMessageContains(logCaptor, "Skipped count");
        }

        DocumentMetadataHandle metadata = getDatabaseClient().newDocumentManager().readMetadata("/test/1.json", new DocumentMetadataHandle());
        assertNotNull(metadata.getMetadataValues().get("incrementalWriteHash"));
        assertFalse(metadata.getMetadataValues().containsKey("incrementalWriteTimestamp"));
        assertEquals(1, metadata.getMetadataValues().size());

        // Write the same documents again and verify documents are skipped instead of written.
        try (LogCaptor logCaptor = LogCaptor.forName(Util.MAIN_LOGGER.getName())) {
            newWriter(1)
                .option(Options.WRITE_INCREMENTAL, true)
                .option(Options.WRITE_BATCH_SIZE, 20)
                .option(Options.WRITE_THREAD_COUNT, 1)
                .option(Options.WRITE_URI_TEMPLATE, "/test/{docNum}.json")
                .option(Options.WRITE_LOG_PROGRESS, 50)
                .option(Options.WRITE_LOG_SKIPPED_DOCUMENTS, 50)
                .save();

            Stream.of(50, 100, 150, 200).forEach(count -> {
                String message = "Documents skipped: " + count;
                verifyMessageWasLogged(logCaptor, message);
            });

            verifyNoMessageContains(logCaptor, "Documents written");
            verifyMessageWasLogged(logCaptor, "Success count: 0");
            verifyMessageWasLogged(logCaptor, "Skipped count: 200");
        }
    }

    @Test
    void customKeyNames() {
        newWriter(1)
            .option(Options.WRITE_URI_TEMPLATE, "/test/{docNum}.json")
            .option(Options.WRITE_INCREMENTAL, true)
            .option(Options.WRITE_INCREMENTAL_HASH_KEY_NAME, "customWriteHash")
            .option(Options.WRITE_INCREMENTAL_TIMESTAMP_KEY_NAME, "customWriteTimestamp")
            .save();

        DocumentMetadataHandle.DocumentMetadataValues metadata = getDatabaseClient().newDocumentManager()
            .readMetadata("/test/1.json", new DocumentMetadataHandle()).getMetadataValues();
        assertNotNull(metadata.get("customWriteHash"));
        assertNotNull(metadata.get("customWriteTimestamp"));
        assertFalse(metadata.containsKey("incrementalWriteHash"), "The default Java Client hash key should not be set.");
        assertFalse(metadata.containsKey("incrementalWriteTimestamp"), "The default Java Client timestamp key should not be set.");
    }

    @Test
    void nullNamesDefaultToJavaClientDefaults() {
        newWriter(1)
            .option(Options.WRITE_URI_TEMPLATE, "/test/{docNum}.json")
            .option(Options.WRITE_INCREMENTAL, true)
            .option(Options.WRITE_INCREMENTAL_HASH_KEY_NAME, null)
            .option(Options.WRITE_INCREMENTAL_TIMESTAMP_KEY_NAME, null)
            .save();

        DocumentMetadataHandle metadata = getDatabaseClient().newDocumentManager().readMetadata("/test/1.json", new DocumentMetadataHandle());
        // These are the defaults as defined by the Java Client.
        assertNotNull(metadata.getMetadataValues().get("incrementalWriteHash"));
        assertFalse(metadata.getMetadataValues().containsKey("incrementalWriteTimestamp"));
        assertEquals(1, metadata.getMetadataValues().size());
    }

    @Test
    void filterErrorShouldNotBeRetried() {
        CommitResultsTestConsumer.reset();

        try {
            newSparkSession().read()
                .parquet("src/test/resources/cars.parquet")
                .write()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.WRITE_INCREMENTAL, true)
                .option(Options.WRITE_INCREMENTAL_HASH_KEY_NAME, "invalid-key-name-should-cause-error")
                // Ensure we get many batches so that we can verify that all rows fail and not a single batch succeeds.
                .option(Options.WRITE_BATCH_SIZE, 2)
                .option(Options.CLIENT_URI, makeClientUri())
                .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
                .option(Options.WRITE_COLLECTIONS, "parquet-test")
                // Setting this to false normally causes the BatchRetrier to kick in and retry failed batches,
                // but it shouldn't due to the filter error being non-retryable.
                .option(Options.WRITE_ABORT_ON_FAILURE, false)
                .option(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME, "com.marklogic.spark.writer.CommitResultsTestConsumer")
                .mode(SaveMode.Append)
                .save();

            assertEquals(0, CommitResultsTestConsumer.successCount.get());
            assertEquals(32, CommitResultsTestConsumer.failureCount.get(), "All rows in the test file should have failed.");
            assertCollectionSize(
                "No data should have been written due to the filter error",
                "parquet-test", 0
            );
        } finally {
            CommitResultsTestConsumer.reset();
        }
    }

    private void verifyMessageWasLogged(LogCaptor logCaptor, String message) {
        assertTrue(
            logCaptor.getInfoLogs().contains(message),
            "Did not find log message: " + message + "; log messages: " + logCaptor.getInfoLogs()
        );
    }

    private void verifyNoMessageContains(LogCaptor logCaptor, String message) {
        assertFalse(
            logCaptor.getInfoLogs().stream().anyMatch(log -> log.contains(message)),
            "Found unexpected log message containing: " + message + "; log messages: " + logCaptor.getInfoLogs()
        );
    }
}
