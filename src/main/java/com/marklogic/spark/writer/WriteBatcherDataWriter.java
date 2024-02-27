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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.TripleRowSchema;
import com.marklogic.spark.writer.rdf.RdfRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Uses the Java Client's WriteBatcher to handle writing rows as documents to MarkLogic.
 */
class WriteBatcherDataWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(WriteBatcherDataWriter.class);

    private final WriteContext writeContext;
    private final DatabaseClient databaseClient;
    private final DataMovementManager dataMovementManager;
    private final WriteBatcher writeBatcher;
    private final DocBuilder docBuilder;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    // Used to capture the first failure that occurs during a request to MarkLogic.
    private final AtomicReference<Throwable> writeFailure;

    private final RowConverter rowConverter;

    // Updated as batches are processed.
    private final AtomicInteger successItemCount = new AtomicInteger(0);
    private final AtomicInteger failedItemCount = new AtomicInteger(0);

    WriteBatcherDataWriter(WriteContext writeContext, int partitionId, long taskId, long epochId) {
        this.writeContext = writeContext;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.writeFailure = new AtomicReference<>();
        this.docBuilder = this.writeContext.newDocBuilder();
        this.databaseClient = writeContext.connectToMarkLogic();
        this.dataMovementManager = this.databaseClient.newDataMovementManager();
        this.writeBatcher = writeContext.newWriteBatcher(this.dataMovementManager);
        addBatchListeners(this.writeBatcher);
        this.rowConverter = determineRowConverter();
        this.dataMovementManager.startJob(this.writeBatcher);
    }

    @Override
    public void write(InternalRow row) throws IOException {
        throwWriteFailureIfExists();
        // We'll need a class here that has a "getLastDoc" method or something like that.
        // It doesn't seem worth creating a new writer just because we have N rows mapping to 1 document now.
        // Though maybe it's worth it as we won't have to mess up CommitMessage with a graph property?
        // Nah, let's keep it here so we can reuse the DMSDK plumbing.
        Optional<DocBuilder.DocumentInputs> document = rowConverter.convertRow(row);
        if (document.isPresent()) {
            this.writeBatcher.add(this.docBuilder.build(document.get()));
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        List<DocBuilder.DocumentInputs> documentInputs = rowConverter.getRemainingDocumentInputs();
        if (documentInputs != null) {
            documentInputs.forEach(inputs -> {
                DocumentWriteOperation writeOp = this.docBuilder.build(inputs);
                this.writeBatcher.add(writeOp);
            });
        }
        this.writeBatcher.flushAndWait();

        throwWriteFailureIfExists();

        // Need this hack so that the complete set of graphs can be reported back to MarkLogicWrite, which handles
        // creating the graphs after all documents have been written.
        Set<String> graphs = null;
        if (this.rowConverter instanceof RdfRowConverter) {
            graphs = ((RdfRowConverter) rowConverter).getGraphs();
        }

        return new CommitMessage(successItemCount.get(), failedItemCount.get(), partitionId, taskId, epochId, graphs);
    }

    @Override
    public void abort() {
        Util.MAIN_LOGGER.warn("Abort called; stopping job");
        stopJobAndRelease();
    }

    @Override
    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug("Close called; stopping job.");
        }
        stopJobAndRelease();
    }

    private void addBatchListeners(WriteBatcher writeBatcher) {
        writeBatcher.onBatchSuccess(batch -> this.successItemCount.getAndAdd(batch.getItems().length));
        if (writeContext.isAbortOnFailure()) {
            // Logging not needed here, as WriteBatcherImpl already logs this at the warning level.
            writeBatcher.onBatchFailure((batch, failure) -> this.writeFailure.compareAndSet(null, failure));
        } else {
            writeBatcher.onBatchFailure((batch, failure) -> this.failedItemCount.getAndAdd(batch.getItems().length));
        }
    }

    private RowConverter determineRowConverter() {
        if (writeContext.isUsingFileSchema()) {
            return new FileRowConverter(writeContext);
        } else if (DocumentRowSchema.SCHEMA.equals(writeContext.getSchema())) {
            return new DocumentRowConverter();
        } else if (TripleRowSchema.SCHEMA.equals(writeContext.getSchema())) {
            return new RdfRowConverter(writeContext);
        }
        return new ArbitraryRowConverter(writeContext);
    }

    private synchronized void throwWriteFailureIfExists() throws IOException {
        if (writeFailure.get() != null) {
            // Only including the message seems sufficient here, as Spark is logging the stacktrace. And the user
            // most likely only needs to know the message.
            throw new IOException(writeFailure.get().getMessage());
        }
    }

    private void stopJobAndRelease() {
        if (this.writeBatcher != null && this.dataMovementManager != null) {
            this.dataMovementManager.stopJob(this.writeBatcher);
        }
        if (this.databaseClient != null) {
            this.databaseClient.release();
        }
    }
}
