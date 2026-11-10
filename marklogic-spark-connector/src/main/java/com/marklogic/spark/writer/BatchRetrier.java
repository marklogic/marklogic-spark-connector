/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteBatchImpl;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.impl.GenericDocumentImpl;
import com.marklogic.spark.Util;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Handles retrying a failed batch from a DMSDK WriteBatcher. The client has no idea how many documents in a batch
 * failed or which ones failed. So this class divides the batch into two and retries each smaller batch, repeating that
 * processing until a batch either succeeds or it fails with a single document in it. Once the latter occurs, this
 * class logs the URI of the document that failed along with the error message.
 */
class BatchRetrier {

    private final GenericDocumentImpl documentManager;
    private final String temporalCollection;
    private final BiConsumer<DocumentWriteOperation, Throwable> failedDocumentConsumer;
    private final Consumer<WriteBatch> successfulBatchConsumer;

    /**
     * @param documentManager         requires the concrete class so that the methods that allow a temporal collection are available.
     *                                MLE-13453 was created to address this.
     * @param temporalCollection      optional temporal collection.
     * @param successfulBatchConsumer client provides an implementation of this to optionally perform any processing after a
     *                                retried batch is successfully written. The batch contains only the documents
     *                                that were written.
     * @param failedDocumentConsumer  client provides an implementation of this to handle whatever logic is required
     *                                when a failed document is identified.
     */
    BatchRetrier(GenericDocumentImpl documentManager, String temporalCollection,
                 Consumer<WriteBatch> successfulBatchConsumer,
                 BiConsumer<DocumentWriteOperation, Throwable> failedDocumentConsumer) {
        this.documentManager = documentManager;
        this.temporalCollection = temporalCollection;
        this.successfulBatchConsumer = successfulBatchConsumer;
        this.failedDocumentConsumer = failedDocumentConsumer;
    }

    /**
     * Intended to be invoked by a DMSDK batch failure listener.
     *
     * @param batch
     * @param failure
     */
    void retryBatch(WriteBatch batch, Throwable failure) {
        divideInHalfAndRetryEachBatch(batch, Arrays.asList(batch.getItems()), failure);
    }

    /**
     * Recursive method that ends when the list of failed events has a size of 1, as there's nothing more to split
     * up and retry. The original WriteEvent objects are retained so that each successfully retried batch is built
     * from the same events that the WriteBatcher created.
     *
     * @param originalBatch the batch that failed in the WriteBatcher, used to construct each successful retried batch
     * @param failedEvents
     */
    private void divideInHalfAndRetryEachBatch(WriteBatch originalBatch, List<WriteEvent> failedEvents, Throwable failure) {
        final int eventCount = failedEvents.size();
        if (eventCount == 1) {
            DocumentWriteOperation failedDoc = toWriteSet(failedEvents).iterator().next();
            this.failedDocumentConsumer.accept(failedDoc, failure);
            return;
        }

        final int half = eventCount / 2;
        for (List<WriteEvent> events : List.of(failedEvents.subList(0, half), failedEvents.subList(half, eventCount))) {
            try {
                this.documentManager.write(toWriteSet(events), null, null, this.temporalCollection);
                safeInvokeSuccessfulBatchConsumer(originalBatch, events);
            } catch (Exception ex) {
                divideInHalfAndRetryEachBatch(originalBatch, events, ex);
            }
        }
    }

    private DocumentWriteSet toWriteSet(List<WriteEvent> events) {
        DocumentWriteSet writeSet = this.documentManager.newWriteSet();
        for (WriteEvent event : events) {
            writeSet.add(event.getTargetUri(), event.getMetadata(), event.getContent());
        }
        return writeSet;
    }

    /**
     * The consumer must never cause a failure, as that would result in documents that were already written being
     * retried. Any error is logged and processing continues.
     */
    private void safeInvokeSuccessfulBatchConsumer(WriteBatch originalBatch, List<WriteEvent> writtenEvents) {
        if (this.successfulBatchConsumer == null) {
            return;
        }
        try {
            this.successfulBatchConsumer.accept(new WriteBatchImpl()
                .withBatcher(originalBatch.getBatcher())
                .withClient(originalBatch.getClient())
                .withJobTicket(originalBatch.getJobTicket())
                // A failed batch can produce multiple retried batches, all of which share the original batch number.
                .withJobBatchNumber(originalBatch.getJobBatchNumber())
                .withTimestamp(Calendar.getInstance())
                .withItems(writtenEvents.toArray(new WriteEvent[0])));
        } catch (Exception ex) {
            Util.MAIN_LOGGER.error("Unable to process successfully retried batch; cause: {}", ex.getMessage(), ex);
        }
    }
}
