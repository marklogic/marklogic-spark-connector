package com.marklogic.spark.reader;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class CustomCodeBatch implements Batch {

    private CustomCodeContext customCodeContext;

    public CustomCodeBatch(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        // We don't yet support partitioning a user's custom code. In the future, we may support this by passing along
        // e.g. host and/or forest names, though the burden would then be on the user to utilize those correctly in
        // their custom code.
        return new InputPartition[]{new CustomCodePartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomCodePartitionReaderFactory(customCodeContext);
    }

}
