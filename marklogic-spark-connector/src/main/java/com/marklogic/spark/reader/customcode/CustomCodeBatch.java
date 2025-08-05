/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.List;

class CustomCodeBatch implements Batch {

    private CustomCodeContext customCodeContext;
    private InputPartition[] inputPartitions;

    CustomCodeBatch(CustomCodeContext customCodeContext, List<String> partitions) {
        this.customCodeContext = customCodeContext;
        if (partitions != null && partitions.size() > 1) {
            inputPartitions = new InputPartition[partitions.size()];
            for (int i = 0; i < partitions.size(); i++) {
                inputPartitions[i] = new CustomCodePartition(partitions.get(i));
            }
        } else {
            inputPartitions = new InputPartition[]{new CustomCodePartition()};
        }
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return inputPartitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomCodePartitionReaderFactory(customCodeContext);
    }

}
