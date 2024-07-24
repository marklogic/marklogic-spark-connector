/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles the progress counter for any operation involving reading from MarkLogic. A Spark job/application can only have
 * one reader, and thus DefaultSource handles resetting this counter before a new read job starts up. A static counter
 * is used so that all reader partitions in the same JVM can have their progress aggregated and logged.
 */
public class ReadProgressLogger extends ProgressLogger {

    public static final AtomicLong progressCounter = new AtomicLong(0);

    public ReadProgressLogger(long progressInterval, int batchSize, String message) {
        super(progressInterval, batchSize, message);
    }

    @Override
    protected long getNewSum(long itemCount) {
        return progressCounter.addAndGet(itemCount);
    }
}
