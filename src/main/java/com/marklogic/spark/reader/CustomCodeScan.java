package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.CustomCodeContext;
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

    public CustomCodeScan(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
        this.partitions = new ArrayList<>();

        if (this.customCodeContext.hasOption(Options.READ_PARTITIONS_INVOKE, Options.READ_PARTITIONS_JAVASCRIPT, Options.READ_PARTITIONS_XQUERY)) {
            DatabaseClient client = this.customCodeContext.connectToMarkLogic();
            try {
                this.customCodeContext
                    .buildCall(client, new CustomCodeContext.CallOptions(
                        Options.READ_PARTITIONS_INVOKE, Options.READ_PARTITIONS_JAVASCRIPT, Options.READ_PARTITIONS_XQUERY
                    ))
                    .eval()
                    .forEach(result -> this.partitions.add(result.getString()));
            } catch (Exception ex) {
                throw new ConnectorException("Unable to retrieve partitions", ex);
            } finally {
                client.release();
            }
        }
    }

    @Override
    public StructType readSchema() {
        return customCodeContext.getSchema();
    }

    @Override
    public Batch toBatch() {
        return new CustomCodeBatch(customCodeContext, partitions);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new CustomCodeMicroBatchStream(customCodeContext, partitions);
    }
}
