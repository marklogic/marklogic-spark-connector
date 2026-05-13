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

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteWithBatchListenerTest extends AbstractWriteTest {

    @Test
    void simpleBatchListener() {
        MyBatchListener.reset();

        newWriter()
            .option(Options.WRITE_BATCH_LISTENER_CLASSNAME, MyBatchListener.class.getName())
            .option(Options.WRITE_BATCH_LISTENER_PARAM_PREFIX + "param1", "value1")
            .option(Options.WRITE_BATCH_LISTENER_PARAM_PREFIX + "param2", "value2")
            .save();

        assertCollectionSize(COLLECTION, 200);
        assertEquals(200, MyBatchListener.itemCount.get(), "200 docs are written, and so are custom " +
            "batch listener should have an itemCount of 200.");

        assertEquals(2, MyBatchListener.constructorParams.size(),
            "The batch listener should have received the two parameters specified in the options");
        assertEquals("value1", MyBatchListener.constructorParams.get("param1"));
        assertEquals("value2", MyBatchListener.constructorParams.get("param2"));

        assertTrue(MyBatchListener.wasClosed, "When the data writer has finished, it needs to see if " +
            "the batch listener is not null and implements Closeable. If so, it should invoke close() " +
            "on it so that the listener has a chance to clean up its resources.");
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
            .option(Options.WRITE_BATCH_LISTENER_CLASSNAME, NotABatchListener.class.getName());

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("does not implement WriteBatchListener"),
            "Should get an immediate failure if the class isn't actually a valid listener; actual error: " + ex.getMessage());
        assertCollectionSize(COLLECTION, 0);
    }

    public static class MyBatchListener implements WriteBatchListener, Closeable {

        public static AtomicInteger itemCount = new AtomicInteger(0);
        public static boolean wasClosed;
        public static Map<String, String> constructorParams;

        public static void reset() {
            itemCount.set(0);
            wasClosed = false;
            constructorParams = null;
        }

        public MyBatchListener(Map<String, String> params) {
            constructorParams = params;
        }

        @Override
        public void processEvent(WriteBatch batch) {
            itemCount.addAndGet(batch.getItems().length);
        }

        @Override
        public void close() {
            wasClosed = true;
        }
    }

    public static class UnhappyBatchListener implements WriteBatchListener {

        public UnhappyBatchListener(Map<String, String> params) {
        }

        @Override
        public void processEvent(WriteBatch batch) {
            throw new RuntimeException("I am not happy.");
        }
    }

    public static class NotABatchListener {
        public NotABatchListener(Map<String, String> params) {
        }
    }
}
