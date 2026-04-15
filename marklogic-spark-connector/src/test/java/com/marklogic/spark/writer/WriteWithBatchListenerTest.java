/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteBatchListener;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteWithBatchListenerTest extends AbstractWriteTest {

    @Test
    void simpleBatchListener() {
        MyBatchListener.itemCount.set(0);

        newWriter()
            .option(Options.WRITE_BATCH_LISTENER_CLASSNAME, MyBatchListener.class.getName())
            .save();

        assertCollectionSize(COLLECTION, 200);
        assertEquals(200, MyBatchListener.itemCount.get(), "200 docs are written, and so are custom " +
            "batch listener should have an itemCount of 200.");
    }

    @Test
    void batchListenerThrowsAnError() {
        newWriter()
            .option(Options.WRITE_BATCH_LISTENER_CLASSNAME, UnhappyBatchListener.class.getName())
            .save();

        assertCollectionSize(
            "The batch listener failure should be logged (DMSDK does this by default) but for now, it should " +
                "not cause the job to fail. We may decide to change this behavior in the future or at least " +
                "make it configurable", COLLECTION, 200);
    }

    @Test
    void notABatchListener() {
        DataFrameWriter<?> writer = newWriter()
            .option(Options.WRITE_BATCH_LISTENER_CLASSNAME, getClass().getName());

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("Failed to instantiate WriteBatchListener class"),
            "Should get an immediate failure if the class isn't actually a valid listener; actual error: " + ex.getMessage());
        assertCollectionSize(COLLECTION, 0);
    }

    public static class MyBatchListener implements WriteBatchListener {

        public static AtomicInteger itemCount = new AtomicInteger(0);

        @Override
        public void processEvent(WriteBatch batch) {
            itemCount.addAndGet(batch.getItems().length);
        }
    }

    public static class UnhappyBatchListener implements WriteBatchListener {

        @Override
        public void processEvent(WriteBatch batch) {
            throw new RuntimeException("I am not happy.");
        }
    }
}
