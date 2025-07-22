/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpticPartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private static final Logger logger = LoggerFactory.getLogger(OpticPartitionReaderFactory.class);
    private static int readerCounter = 0;
    private final OpticReadContext opticReadContext;

    OpticPartitionReaderFactory(OpticReadContext opticReadContext) {
        logger.info("CREATED");
        this.opticReadContext = opticReadContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        int currentReaderId = ++readerCounter;
        logger.info("createReader() called - creating reader #{} for partition: {}", currentReaderId, partition);
        if (logger.isDebugEnabled()) {
            logger.debug("Creating reader for partition: {}", partition);
        }
        return new OpticPartitionReader(this.opticReadContext, (PlanAnalysis.Partition) partition);
    }
}
