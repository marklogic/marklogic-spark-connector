/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;

/**
 * Stateful class that is intended to be used in a singleton manner with synchronized access to its one method.
 */
class ProgressLogger implements Serializable {

    static final long serialVersionUID = 1L;

    private final long progressInterval;
    private final String message;

    private long progressCounter;
    private long nextProgressInterval;

    ProgressLogger(long progressInterval, String message) {
        this.progressInterval = progressInterval;
        this.message = message;
        this.nextProgressInterval = progressInterval;
    }

    void logProgressIfNecessary(long itemCount) {
        if (Util.MAIN_LOGGER.isInfoEnabled() && progressInterval > 0) {
            this.progressCounter += itemCount;
            if (progressCounter >= nextProgressInterval) {
                Util.MAIN_LOGGER.info(message, nextProgressInterval);
                nextProgressInterval += progressInterval;
            }
        }
    }
}
