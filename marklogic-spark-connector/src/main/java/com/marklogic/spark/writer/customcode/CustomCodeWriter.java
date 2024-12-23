/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.*;
import com.marklogic.spark.reader.customcode.CustomCodeContext;
import com.marklogic.spark.writer.CommitMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class CustomCodeWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(CustomCodeWriter.class);

    private final DatabaseClient databaseClient;
    private final CustomCodeContext customCodeContext;
    private final JsonRowSerializer jsonRowSerializer;
    private final int batchSize;

    private final List<String> currentBatch = new ArrayList<>();
    private final String externalVariableDelimiter;
    private ObjectMapper objectMapper;

    // Only used for logging.
    private final int partitionId;
    private final long taskId;

    // Updated after each call to MarkLogic.
    private int successItemCount;
    private int failedItemCount;

    CustomCodeWriter(CustomCodeContext customCodeContext, int partitionId, long taskId) {
        this.customCodeContext = customCodeContext;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.databaseClient = customCodeContext.connectToMarkLogic();
        this.jsonRowSerializer = new JsonRowSerializer(customCodeContext.getSchema(), customCodeContext.getProperties());

        this.batchSize = customCodeContext.getIntOption(Options.WRITE_BATCH_SIZE, 1, 1);

        this.externalVariableDelimiter = customCodeContext.optionExists(Options.WRITE_EXTERNAL_VARIABLE_DELIMITER) ?
            customCodeContext.getProperties().get(Options.WRITE_EXTERNAL_VARIABLE_DELIMITER) : ",";

        if (this.customCodeContext.isCustomSchema() && this.batchSize > 1) {
            this.objectMapper = new ObjectMapper();
        }
    }

    @Override
    public void write(InternalRow row) {
        String rowValue = customCodeContext.isCustomSchema() ?
            jsonRowSerializer.serializeRowToJson(row) :
            row.getString(0);

        this.currentBatch.add(rowValue);
        if (this.currentBatch.size() >= this.batchSize) {
            flush();
        }
    }

    @Override
    public WriterCommitMessage commit() {
        flush();
        CommitMessage message = new CommitMessage(successItemCount, failedItemCount, null);
        if (logger.isDebugEnabled()) {
            logger.debug("Committing {}", message);
        }
        return message;
    }

    @Override
    public void abort() {
        // No action to take.
    }

    @Override
    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug("Close called.");
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    /**
     * Handles making a call to MarkLogic once the current batch is full, or when Spark calls commit(), which ensures
     * that any partial batch is still processed.
     */
    private void flush() {
        if (currentBatch.isEmpty()) {
            return;
        }

        final int itemCount = currentBatch.size();
        ServerEvaluationCall call = customCodeContext.buildCall(
            this.databaseClient,
            new CustomCodeContext.CallOptions(Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY,
                Options.WRITE_JAVASCRIPT_FILE, Options.WRITE_XQUERY_FILE)
        );
        call.addVariable(determineExternalVariableName(), makeVariableValue());
        currentBatch.clear();
        executeCall(call, itemCount);
    }

    private String determineExternalVariableName() {
        return customCodeContext.optionExists(Options.WRITE_EXTERNAL_VARIABLE_NAME) ?
            customCodeContext.getProperties().get(Options.WRITE_EXTERNAL_VARIABLE_NAME) :
            "URI";
    }

    private AbstractWriteHandle makeVariableValue() {
        if (customCodeContext.isCustomSchema()) {
            return this.batchSize == 1 ?
                new StringHandle(currentBatch.get(0)).withFormat(Format.JSON) :
                new JacksonHandle(makeArrayFromCurrentBatch());
        }
        // This works fine regardless of the batch size.
        final String uriValue = currentBatch.stream().collect(Collectors.joining(externalVariableDelimiter));
        return new StringHandle(uriValue).withFormat(Format.TEXT);
    }

    private ArrayNode makeArrayFromCurrentBatch() {
        ArrayNode array = this.objectMapper.createArrayNode();
        for (String jsonObjectString : currentBatch) {
            try {
                array.add(this.objectMapper.readTree(jsonObjectString));
            } catch (JsonProcessingException e) {
                throw new ConnectorException(String.format("Unable to read JSON string while constructing call " +
                    "for multiple rows with a custom schema; JSON: %s", jsonObjectString), e);
            }
        }
        return array;
    }

    private void executeCall(ServerEvaluationCall call, int itemCount) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            // Helps ensure that the proper number of partitions and tasks/threads are being used by Spark.
            Util.MAIN_LOGGER.debug("Processing items, count: {}; partition: {}; task: {}", itemCount, partitionId, taskId);
        }
        try {
            call.evalAs(String.class);
            this.successItemCount += itemCount;
            WriteProgressLogger.logProgressIfNecessary(itemCount);
        } catch (RuntimeException ex) {
            if (customCodeContext.isAbortOnFailure()) {
                throw ex;
            }
            this.failedItemCount += itemCount;
            Util.MAIN_LOGGER.error(String.format("Unable to process row; cause: %s", ex.getMessage()));
        }
    }
}
