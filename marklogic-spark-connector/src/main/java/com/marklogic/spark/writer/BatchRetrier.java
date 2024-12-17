/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.impl.GenericDocumentImpl;

import java.util.Iterator;
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
    private final Consumer<DocumentWriteSet> successfulBatchConsumer;

    /**
     * @param documentManager         requires the concrete class so that the methods that allow a temporal collection are available.
     *                                MLE-13453 was created to address this.
     * @param temporalCollection      optional temporal collection.
     * @param successfulBatchConsumer client provides an implementation of this to optionally perform any processing after a
     *                                batch is successfully written.
     * @param failedDocumentConsumer  client provides an implementation of this to handle whatever logic is required
     *                                when a failed document is identified.
     */
    BatchRetrier(GenericDocumentImpl documentManager, String temporalCollection,
                 Consumer<DocumentWriteSet> successfulBatchConsumer,
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
        DocumentWriteSet writeSet = this.documentManager.newWriteSet();
        for (WriteEvent item : batch.getItems()) {
            writeSet.add(item.getTargetUri(), item.getMetadata(), item.getContent());
        }
        divideInHalfAndRetryEachBatch(writeSet, failure);
    }

    /**
     * Recursive method that ends when the set of failed documents has a size of 1, as there's nothing more to split
     * up and retry.
     *
     * @param failedWriteSet
     */
    private void divideInHalfAndRetryEachBatch(DocumentWriteSet failedWriteSet, Throwable failure) {
        final int docCount = failedWriteSet.size();
        if (docCount == 1) {
            DocumentWriteOperation failedDoc = failedWriteSet.iterator().next();
            this.failedDocumentConsumer.accept(failedDoc, failure);
            return;
        }

        DocumentWriteSet newWriteSet = this.documentManager.newWriteSet();
        Iterator<DocumentWriteOperation> failedDocs = failedWriteSet.iterator();
        while (failedDocs.hasNext()) {
            newWriteSet.add(failedDocs.next());
            if (newWriteSet.size() == docCount / 2 || !failedDocs.hasNext()) {
                try {
                    this.documentManager.write(newWriteSet, null, null, this.temporalCollection);
                    if (this.successfulBatchConsumer != null) {
                        this.successfulBatchConsumer.accept(newWriteSet);
                    }
                } catch (Exception ex) {
                    divideInHalfAndRetryEachBatch(newWriteSet, ex);
                }
                newWriteSet = this.documentManager.newWriteSet();
            }
        }
    }
}
