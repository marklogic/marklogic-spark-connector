package com.marklogic.spark.reader.customcode;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.List;

class CustomCodeBatch implements Batch {

    private CustomCodeContext customCodeContext;
    private List<String> partitions;

    public CustomCodeBatch(CustomCodeContext customCodeContext, List<String> partitions) {
        this.customCodeContext = customCodeContext;
        this.partitions = partitions;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] inputPartitions;
        if (partitions != null && partitions.size() > 1) {
            inputPartitions = new InputPartition[partitions.size()];
            for (int i = 0; i < partitions.size(); i++) {
                inputPartitions[i] = new CustomCodePartition(partitions.get(i));
            }
        } else {
            inputPartitions = new InputPartition[]{new CustomCodePartition()};
        }
        return inputPartitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomCodePartitionReaderFactory(customCodeContext);
    }

}
