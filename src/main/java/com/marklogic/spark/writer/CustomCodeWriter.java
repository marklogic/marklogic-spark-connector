package com.marklogic.spark.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.CustomCodeContext;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class CustomCodeWriter implements DataWriter<InternalRow> {

    private final static Logger logger = LoggerFactory.getLogger(CustomCodeWriter.class);

    private final DatabaseClient databaseClient;
    private final CustomCodeContext customCodeContext;

    private final int batchSize;
    private final List<String> currentBatch = new ArrayList<>();
    private final String externalVariableDelimiter;
    private ObjectMapper objectMapper;

    // Only used for commit messages
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    CustomCodeWriter(CustomCodeContext customCodeContext, int partitionId, long taskId, long epochId) {
        this.customCodeContext = customCodeContext;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;

        this.databaseClient = customCodeContext.connectToMarkLogic();

        this.batchSize = customCodeContext.optionExists(Options.WRITE_BATCH_SIZE) ?
            Integer.parseInt(customCodeContext.getProperties().get(Options.WRITE_BATCH_SIZE)) : 1;

        this.externalVariableDelimiter = customCodeContext.optionExists(Options.WRITE_EXTERNAL_VARIABLE_DELIMITER) ?
            customCodeContext.getProperties().get(Options.WRITE_EXTERNAL_VARIABLE_DELIMITER) : ",";

        if (this.customCodeContext.isCustomSchema() && this.batchSize > 1) {
            this.objectMapper = new ObjectMapper();
        }
    }

    @Override
    public void write(InternalRow record) {
        String rowValue = customCodeContext.isCustomSchema() ?
            convertRowToJSONString(record) :
            record.getString(0);

        this.currentBatch.add(rowValue);

        if (this.currentBatch.size() >= this.batchSize) {
            flush();
        }
    }

    @Override
    public WriterCommitMessage commit() {
        flush();
        CommitMessage message = new CommitMessage(partitionId, taskId, epochId);
        if (logger.isDebugEnabled()) {
            logger.debug("Committing {}", message);
        }
        return message;
    }

    @Override
    public void abort() {
        logger.warn("Abort called; stopping job");
    }

    @Override
    public void close() {
        logger.info("Close called; stopping job");
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

        if (logger.isDebugEnabled()) {
            logger.debug("Calling custom code in MarkLogic");
        }
        ServerEvaluationCall call = customCodeContext.buildCall(
            this.databaseClient,
            new CustomCodeContext.CallOptions(Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY)
        );
        call.addVariable(determineExternalVariableName(), makeVariableValue());
        currentBatch.clear();
        executeCall(call);
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
                throw new RuntimeException(String.format("Unable to read JSON string while constructing call " +
                    "for multiple rows with a custom schema; JSON: %s", jsonObjectString), e);
            }
        }
        return array;
    }

    private String convertRowToJSONString(InternalRow record) {
        StringWriter jsonObjectWriter = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
            this.customCodeContext.getSchema(),
            jsonObjectWriter,
            Util.DEFAULT_JSON_OPTIONS
        );
        jacksonGenerator.write(record);
        jacksonGenerator.flush();
        return jsonObjectWriter.toString();
    }

    private void executeCall(ServerEvaluationCall call) {
        try {
            call.evalAs(String.class);
        } catch (RuntimeException ex) {
            if (customCodeContext.isAbortOnFailure()) {
                throw ex;
            }
            logger.error(String.format("Unable to process row; cause: %s", ex.getMessage()));
        }
    }

    private static class CommitMessage implements WriterCommitMessage {
        private int partitionId;
        private long taskId;
        private long epochId;

        public CommitMessage(int partitionId, long taskId, long epochId) {
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.epochId = epochId;
        }

        @Override
        public String toString() {
            return epochId != 0L ?
                String.format("[partitionId: %d; taskId: %d; epochId: %d]", partitionId, taskId, epochId) :
                String.format("[partitionId: %d; taskId: %d]", partitionId, taskId);
        }
    }
}
