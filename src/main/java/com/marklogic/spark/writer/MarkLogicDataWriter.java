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
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class MarkLogicDataWriter implements DataWriter<InternalRow> {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicDataWriter.class);

    private final WriteContext writeContext;
    private final DatabaseClient databaseClient;
    private final DataMovementManager dataMovementManager;
    private final WriteBatcher writeBatcher;
    private final int partitionId;
    private final long taskId;

    // Used to capture the first failure that occurs during a request to MarkLogic.
    private final AtomicReference<Throwable> writeFailure;

    private int docCount;

    MarkLogicDataWriter(WriteContext writeContext, int partitionId, long taskId) {
        this.writeContext = writeContext;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.writeFailure = new AtomicReference<>();

        this.databaseClient = writeContext.connectToMarkLogic();
        this.dataMovementManager = this.databaseClient.newDataMovementManager();
        this.writeBatcher = writeContext.newWriteBatcher(this.dataMovementManager);
        this.writeBatcher.onBatchFailure((batch, failure) -> {
            // Logging not needed here, as WriteBatcherImpl already logs this at the warning level.
            this.writeFailure.compareAndSet(null, failure);
        });
        this.dataMovementManager.startJob(this.writeBatcher);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        throwWriteFailureIfExists();
        // TODO Make the URI configurable, like in MLCP
        final String uri = "/test/" + UUID.randomUUID() + ".json";
        String json = convertRowToJSONString(record);
        this.writeBatcher.add(uri, new StringHandle(json).withFormat(Format.JSON));
        this.docCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Committing; partitionId: {}; taskId: {}; document count: {}",
                partitionId, taskId, docCount);
        }
        this.writeBatcher.flushAndWait();
        throwWriteFailureIfExists();
        return new MarkLogicCommitMessage(docCount, partitionId, taskId);
    }

    @Override
    public void abort() {
        logger.warn("Abort called; stopping job");
        stopJobAndRelease();
    }

    @Override
    public void close() {
        logger.info("Close called; stopping job");
        stopJobAndRelease();
    }

    private String convertRowToJSONString(InternalRow record) {
        StringWriter jsonObjectWriter = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
            this.writeContext.getSchema(),
            jsonObjectWriter,
            Util.DEFAULT_JSON_OPTIONS
        );
        jacksonGenerator.write(record);
        jacksonGenerator.flush();
        return jsonObjectWriter.toString();
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

    private static class MarkLogicCommitMessage implements WriterCommitMessage {
        private int docCount;
        private int partitionId;
        private long taskId;

        public MarkLogicCommitMessage(int docCount, int partitionId, long taskId) {
            this.docCount = docCount;
            this.partitionId = partitionId;
            this.taskId = taskId;
        }

        @Override
        public String toString() {
            return String.format("partitionId: %d; taskId: %d; docCount: %d", partitionId, taskId, docCount);
        }
    }
}
