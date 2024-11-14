/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpticBatch implements Batch {

    private static final Logger logger = LoggerFactory.getLogger(OpticBatch.class);

    private final OpticReadContext opticReadContext;
    private final InputPartition[] partitions;

    OpticBatch(OpticReadContext opticReadContext) {
        this.opticReadContext = opticReadContext;
        PlanAnalysis planAnalysis = opticReadContext.getPlanAnalysis();
        partitions = planAnalysis != null ?
            planAnalysis.getPartitionArray() :
            new InputPartition[]{};
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        if (logger.isTraceEnabled()) {
            logger.trace("Creating new partition reader factory");
        }
        return new OpticPartitionReaderFactory(opticReadContext);
    }
}
