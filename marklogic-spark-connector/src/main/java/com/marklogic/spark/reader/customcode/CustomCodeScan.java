/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.ServerEvaluationCallFactory;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

class CustomCodeScan implements Scan {

    private CustomCodeContext customCodeContext;
    private final List<String> partitions;
    private final CustomCodeBatch batch;

    public CustomCodeScan(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
        this.partitions = new ArrayList<>();

        new ServerEvaluationCallFactory.Builder()
            .withInvokeOptionName(Options.READ_PARTITIONS_INVOKE)
            .withJavascriptOptionName(Options.READ_PARTITIONS_JAVASCRIPT)
            .withXqueryOptionName(Options.READ_PARTITIONS_XQUERY)
            .withJavascriptFileOptionName(Options.READ_PARTITIONS_JAVASCRIPT_FILE)
            .withXqueryFileOptionName(Options.READ_PARTITIONS_XQUERY_FILE)
            .withVarsPrefix(Options.READ_VARS_PREFIX)
            .build(this.customCodeContext)
            .ifPresent(callFactory -> {
                try (DatabaseClient client = this.customCodeContext.connectToMarkLogic()) {
                    ServerEvaluationCall call = callFactory.newCall(client);
                    try (EvalResultIterator iter = call.eval()) {
                        iter.forEach(result -> this.partitions.add(result.getString()));
                    } catch (Exception ex) {
                        throw new ConnectorException(String.format("Unable to retrieve partitions; cause: %s", ex.getMessage()), ex);
                    }
                }
            });

        batch = new CustomCodeBatch(customCodeContext, partitions);
    }

    @Override
    public StructType readSchema() {
        return customCodeContext.getSchema();
    }

    @Override
    public Batch toBatch() {
        return batch;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new CustomCodeMicroBatchStream(customCodeContext, partitions);
    }
}
