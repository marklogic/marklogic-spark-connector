/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.spark.Options;
import com.marklogic.spark.api.WriteListener;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class WritePartialBatchTest extends AbstractWriteTest {

    @Test
    void threeOutOfFourShouldFail() {
        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files")
            .repartition(1) // Forces all 4 docs to be written in a single batch.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
        );

        assertCollectionSize("Only the JSON document should have succeeded; error messages should have been logged " +
            "for the other 3 documents.", "partial-batch", 1);
    }

    @Test
    void shouldThrowError() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append);

        SparkException ex = assertThrows(SparkException.class, writer::save);
        assertTrue(ex.getMessage().contains("Document is not JSON"), "Verifying that trying to write non-JSON " +
            "documents with a .json extension should produce an error; unexpected error: " + ex.getMessage());
    }

    @Test
    void fiveOutOfTenShouldFailWithTwoBatches() {
        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/some-bad-json-docs.zip")
            .repartition(1) // Forces a single partition writer.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_REPLACE, ".*/spark-json,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_BATCH_SIZE, 5) // Forces two batches, each of which should have 1 or more failures.
        );

        assertCollectionSize(
            "5 of the docs in the zip file are valid JSON docs, so those should all be written, regardless of how " +
                "many partition writers and batches are created. Errors should be logged for the 5 invalid JSON docs.",
            "partial-batch", 5
        );
    }

    @Test
    void recoveredDocumentsAreSentToWriteListener() {
        UriRecordingWriteListener.reset();

        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/some-bad-json-docs.zip")
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_REPLACE, ".*/spark-json,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_BATCH_SIZE, 5)
            .option(Options.WRITE_LISTENER_CLASS_NAME, UriRecordingWriteListener.class.getName())
        );

        List<String> writtenUris = getUrisInCollection("partial-batch", 5).stream().sorted().toList();
        List<String> notifiedUris = UriRecordingWriteListener.uris.stream().sorted().toList();
        assertEquals(writtenUris, notifiedUris, "Each document written by the batch retrier should be sent to " +
            "the write listener exactly once, and failed documents should not be sent to it.");
    }

    @Test
    void completelyInvalidBatchSendsNothingToWriteListener() {
        UriRecordingWriteListener.reset();

        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files")
            .filter("URI NOT LIKE '%.json'")
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_LISTENER_CLASS_NAME, UriRecordingWriteListener.class.getName())
        );

        assertCollectionSize("partial-batch", 0);
        assertTrue(UriRecordingWriteListener.uris.isEmpty(), "No document was written, so the write listener " +
            "should not have been notified of any; actual URIs: " + UriRecordingWriteListener.uris);
    }

    public static class UriRecordingWriteListener implements WriteListener {

        static final Queue<String> uris = new ConcurrentLinkedQueue<>();

        static void reset() {
            uris.clear();
        }

        public UriRecordingWriteListener(Map<String, String> params) {
        }

        @Override
        public void onBatchSuccess(WriteBatch batch) {
            for (WriteEvent item : batch.getItems()) {
                uris.add(item.getTargetUri());
            }
        }
    }
}
