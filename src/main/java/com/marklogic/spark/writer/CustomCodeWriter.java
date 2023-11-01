package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
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

public class CustomCodeWriter implements DataWriter<InternalRow> {

    private final static Logger logger = LoggerFactory.getLogger(CustomCodeWriter.class);

    private final DatabaseClient databaseClient;
    private final CustomCodeContext customCodeContext;

    public CustomCodeWriter(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
        this.databaseClient = customCodeContext.connectToMarkLogic();
    }

    @Override
    public void write(InternalRow record) {
        ServerEvaluationCall call = customCodeContext.buildCall(
            this.databaseClient, Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY
        );
        try {
            addVariableToCall(record, call);
            call.evalAs(String.class);
        } catch (RuntimeException ex) {
            if (customCodeContext.isAbortOnFailure()) {
                throw ex;
            }
            logger.error(String.format("Unable to process row; cause: %s", ex.getMessage()));
        }
    }

    private void addVariableToCall(InternalRow record, ServerEvaluationCall call) {
        final String variableName = customCodeContext.optionExists(Options.WRITE_EXTERNAL_VARIABLE_NAME) ?
            customCodeContext.getProperties().get(Options.WRITE_EXTERNAL_VARIABLE_NAME) : "URI";

        if (customCodeContext.isCustomSchema()) {
            String jsonObject = convertRowToJSONString(record);
            call.addVariable(variableName, new StringHandle(jsonObject).withFormat(Format.JSON));
        } else {
            String uriValue = record.getString(0);
            call.addVariable(variableName, new StringHandle(uriValue).withFormat(Format.TEXT));
        }
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

    @Override
    public WriterCommitMessage commit() {
        // TODO Capture responses from each eval call?
        return null;
    }

    @Override
    public void abort() {
        // Nothing to do here, the client will be released when Spark calls close().
    }

    @Override
    public void close() {
        logger.warn("CLOSE!");
        if (databaseClient != null) {
            databaseClient.release();
        }
    }
}
