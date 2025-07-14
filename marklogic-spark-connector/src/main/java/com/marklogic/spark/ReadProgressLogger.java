/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;

/**
 * Handles the progress counter for any operation involving reading from MarkLogic. A Spark job/application can only have
 * one reader, and thus DefaultSource handles resetting this counter before a new read job starts up. A static counter
 * is used so that all reader partitions in the same JVM can have their progress aggregated and logged.
 */
public class ReadProgressLogger implements Serializable {

    static final long serialVersionUID = 1L;

    private static final Object lock = new Object();
    private static ProgressLogger progressLogger;

    public static void initialize(long progressInterval, String message) {
        progressLogger = new ProgressLogger(progressInterval, message);
    }

    public static void logProgressIfNecessary(long itemCount) {
        if (Util.MAIN_LOGGER.isInfoEnabled() && progressLogger != null) {
            synchronized (lock) {
                progressLogger.logProgressIfNecessary(itemCount);
            }
        }
    }
}
