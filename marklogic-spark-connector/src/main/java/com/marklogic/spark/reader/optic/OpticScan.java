/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */

package com.marklogic.spark.reader.optic;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpticScan implements Scan {

    private static final Logger logger = LoggerFactory.getLogger(OpticScan.class);

    private OpticReadContext opticReadContext;

    OpticScan(OpticReadContext opticReadContext) {
        this.opticReadContext = opticReadContext;
    }

    @Override
    public StructType readSchema() {
        return opticReadContext.getSchema();
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        if (logger.isTraceEnabled()) {
            logger.trace("Creating new batch");
        }
        return new OpticBatch(opticReadContext);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new OpticMicroBatchStream(opticReadContext);
    }
}
