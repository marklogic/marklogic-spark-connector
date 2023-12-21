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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Uses the Java Client's WriteBatcher to handle writing rows as documents to MarkLogic.
 */
class WriteBatcherDataWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(WriteBatcherDataWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
    }

    @Override
    public void write(InternalRow row) throws IOException {
        throwWriteFailureIfExists();
        DocBuilder.DocumentInputs inputs = writeContext.isUsingFileSchema() ?
            makeDocumentInputsForFileRow(row) :
            makeDocumentInputsForArbitraryRow(row);
        this.writeBatcher.add(this.docBuilder.build(inputs));
        this.docCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        CommitMessage message = new CommitMessage(docCount, partitionId, taskId, epochId);
        if (logger.isDebugEnabled()) {
            logger.debug("Committing {}", message);
        }
        this.writeBatcher.flushAndWait();
        throwWriteFailureIfExists();
        return message;
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

    private DocBuilder.DocumentInputs makeDocumentInputsForArbitraryRow(InternalRow row) {
        String json = convertRowToJSONString(row);
        StringHandle content = new StringHandle(json).withFormat(Format.JSON);
        ObjectNode columnValues = null;
        if (writeContext.hasOption(Options.WRITE_URI_TEMPLATE)) {
            try {
                columnValues = (ObjectNode) objectMapper.readTree(json);
            } catch (JsonProcessingException e) {
                throw new ConnectorException(String.format("Unable to read JSON row: %s", e.getMessage()), e);
            }
        }
        return new DocBuilder.DocumentInputs(null, content, columnValues);
    }

    private DocBuilder.DocumentInputs makeDocumentInputsForFileRow(InternalRow row) {
        String initialUri = row.getString(writeContext.getFileSchemaPathPosition());
        BytesHandle content = new BytesHandle(row.getBinary(writeContext.getFileSchemaContentPosition()));
        forceFormatIfNecessary(content);
        ObjectNode columnValues = null;
        if (writeContext.hasOption(Options.WRITE_URI_TEMPLATE)) {
            columnValues = objectMapper.createObjectNode();
            columnValues.put("path", initialUri);
            Object modificationTime = row.get(1, DataTypes.LongType);
            if (modificationTime != null) {
                columnValues.put("modificationTime", modificationTime.toString());
            }
            columnValues.put("length", row.getLong(2));
            // Not including content as it's a byte array that is not expected to be helpful for making a URI.
        }
        return new DocBuilder.DocumentInputs(initialUri, content, columnValues);
    }

    private void forceFormatIfNecessary(BytesHandle content) {
        if (writeContext.hasOption(Options.WRITE_FILES_DOCUMENT_TYPE)) {
            String value = writeContext.getProperties().get(Options.WRITE_FILES_DOCUMENT_TYPE);
            try {
                content.withFormat(Format.valueOf(value.toUpperCase()));
            } catch (IllegalArgumentException e) {
                String message = "Invalid value for option %s: %s; must be one of 'JSON', 'XML', or 'TEXT'.";
                throw new ConnectorException(String.format(message, Options.WRITE_FILES_DOCUMENT_TYPE, value));
            }
        }
    }

    private String convertRowToJSONString(InternalRow row) {
        StringWriter jsonObjectWriter = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
            this.writeContext.getSchema(),
            jsonObjectWriter,
            Util.DEFAULT_JSON_OPTIONS
        );
        jacksonGenerator.write(row);
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

    private static class CommitMessage implements WriterCommitMessage {
        private int docCount;
        private int partitionId;
        private long taskId;
        private long epochId;

        public CommitMessage(int docCount, int partitionId, long taskId, long epochId) {
            this.docCount = docCount;
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.epochId = epochId;
        }

        @Override
        public String toString() {
            return epochId != 0L ?
                String.format("[partitionId: %d; taskId: %d; epochId: %d]; docCount: %d", partitionId, taskId, epochId, docCount) :
                String.format("[partitionId: %d; taskId: %d]; docCount: %d", partitionId, taskId, docCount);
        }
    }
}
