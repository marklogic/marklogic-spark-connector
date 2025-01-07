/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
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

        if (this.customCodeContext.hasPartitionCode()) {
            DatabaseClient client = this.customCodeContext.connectToMarkLogic();
            ServerEvaluationCall call = this.customCodeContext
                .buildCall(client, new CustomCodeContext.CallOptions(
                    Options.READ_PARTITIONS_INVOKE, Options.READ_PARTITIONS_JAVASCRIPT, Options.READ_PARTITIONS_XQUERY,
                    Options.READ_PARTITIONS_JAVASCRIPT_FILE, Options.READ_PARTITIONS_XQUERY_FILE
                ));
            try (EvalResultIterator iter = call.eval()) {
                iter.forEach(result -> this.partitions.add(result.getString()));
            } catch (Exception ex) {
                throw new ConnectorException(String.format("Unable to retrieve partitions; cause: %s", ex.getMessage()), ex);
            } finally {
                client.release();
            }
        }

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
