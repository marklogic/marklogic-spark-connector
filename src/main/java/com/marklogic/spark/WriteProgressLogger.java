/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WriteProgressLogger extends ProgressLogger {

    public static final AtomicLong progressCounter = new AtomicLong(0);

    public WriteProgressLogger(long progressInterval, int batchSize, String message) {
        super(progressInterval, batchSize, message);
    }

    @Override
    protected long getNewSum(long itemCount) {
        return progressCounter.addAndGet(itemCount);
    }
}
