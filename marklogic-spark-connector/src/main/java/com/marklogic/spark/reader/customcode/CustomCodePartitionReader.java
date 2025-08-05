/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.Options;
import com.marklogic.spark.ReadProgressLogger;
import com.marklogic.spark.reader.JsonRowDeserializer;
import com.marklogic.spark.core.ServerEvaluationCallFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

class CustomCodePartitionReader implements PartitionReader<InternalRow> {

    private final ServerEvaluationCall serverEvaluationCall;
    private final boolean isCustomSchema;

    private EvalResultIterator evalResultIterator;
    private final JsonRowDeserializer jsonRowDeserializer;
    private final DatabaseClient databaseClient;

    // Only needed for logging progress.
    private final long batchSize;
    private long progressCounter;

    public CustomCodePartitionReader(CustomCodeContext customCodeContext, String partition) {
        this.databaseClient = customCodeContext.connectToMarkLogic();
        this.serverEvaluationCall = new ServerEvaluationCallFactory.Builder()
            .withInvokeOptionName(Options.READ_INVOKE)
            .withJavascriptOptionName(Options.READ_JAVASCRIPT)
            .withXqueryOptionName(Options.READ_XQUERY)
            .withJavascriptFileOptionName(Options.READ_JAVASCRIPT_FILE)
            .withXqueryFileOptionName(Options.READ_XQUERY_FILE)
            .withVarsPrefix(Options.READ_VARS_PREFIX)
            .mustBuild(customCodeContext)
            .newCall(this.databaseClient);

        if (partition != null) {
            this.serverEvaluationCall.addVariable("PARTITION", partition);
        }

        this.batchSize = customCodeContext.getNumericOption(Options.READ_BATCH_SIZE, 1, 1);

        this.isCustomSchema = customCodeContext.isCustomSchema();
        this.jsonRowDeserializer = new JsonRowDeserializer(customCodeContext.getSchema());
    }

    @Override
    public boolean next() {
        if (this.evalResultIterator == null) {
            this.evalResultIterator = this.serverEvaluationCall.eval();
        }
        return this.evalResultIterator.hasNext();
    }

    @Override
    public InternalRow get() {
        String val = this.evalResultIterator.next().getString();
        if (this.isCustomSchema) {
            return this.jsonRowDeserializer.deserializeJson(val);
        }
        progressCounter++;
        if (progressCounter >= batchSize) {
            ReadProgressLogger.logProgressIfNecessary(progressCounter);
            progressCounter = 0;
        }
        return new GenericInternalRow(new Object[]{UTF8String.fromString(val)});
    }

    @Override
    public void close() {
        if (this.evalResultIterator != null) {
            this.evalResultIterator.close();
        }
        if (this.databaseClient != null) {
            this.databaseClient.release();
        }
    }
}
