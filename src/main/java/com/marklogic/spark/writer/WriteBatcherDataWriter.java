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
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

    private final Function<InternalRow, DocBuilder.DocumentInputs> rowToDocumentFunction;

    private int docCount;

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
        if (writeContext.isAbortOnFailure()) {
            this.writeBatcher.onBatchFailure((batch, failure) ->
                // Logging not needed here, as WriteBatcherImpl already logs this at the warning level.
                this.writeFailure.compareAndSet(null, failure)
            );
        }
        this.dataMovementManager.startJob(this.writeBatcher);

        if (writeContext.isUsingFileSchema()) {
            rowToDocumentFunction = new FileRowFunction(writeContext);
        } else if (DocumentRowSchema.SCHEMA.equals(writeContext.getSchema())) {
            rowToDocumentFunction = new DocumentRowFunction();
        } else {
            rowToDocumentFunction = new ArbitraryRowFunction(writeContext);
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        throwWriteFailureIfExists();
        DocBuilder.DocumentInputs inputs = rowToDocumentFunction.apply(row);
        this.writeBatcher.add(this.docBuilder.build(inputs));
        this.docCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        CommitMessage message = new CommitMessage("Wrote", docCount, partitionId, taskId, epochId);
        if (logger.isDebugEnabled()) {
            logger.debug("Committing {}", message);
        }
        this.writeBatcher.flushAndWait();
        throwWriteFailureIfExists();
        return message;
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
