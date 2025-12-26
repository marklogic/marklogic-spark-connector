/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncrementalWriteTest extends AbstractWriteTest {

    @Test
    void test() {
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
            });
        }

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
