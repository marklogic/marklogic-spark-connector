/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;

public abstract class ProgressLogger implements Serializable {

    static final long serialVersionUID = 1;

    private final long progressInterval;
    private final int batchSize;
    private final String message;

    protected ProgressLogger(long progressInterval, int batchSize, String message) {
        this.progressInterval = progressInterval;
        this.batchSize = batchSize;
        this.message = message;
    }

    protected abstract long getNewSum(long itemCount);

    public void logProgressIfNecessary(long itemCount) {
        if (this.progressInterval > 0) {
            long sum = getNewSum(itemCount);
            if (sum >= progressInterval) {
                long lowerBound = sum / (this.progressInterval);
                long upperBound = (lowerBound * this.progressInterval) + this.batchSize;
                if (sum >= lowerBound && sum < upperBound) {
                    Util.MAIN_LOGGER.info(message, sum);
                }
            }
        }
    }
}
