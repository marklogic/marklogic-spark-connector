/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;

/**
 * Handles the progress counter for any operation involving writing to MarkLogic. A Spark job/application can only have
 * one writer, and thus DefaultSource handles resetting this counter before a new write job starts up. A static counter
 * is used so that all writer partitions in the same JVM can have their progress aggregated and logged.
 */
public class WriteProgressLogger implements Serializable {

    static final long serialVersionUID = 1L;

    private static final Object lock = new Object();
    private static ProgressLogger progressLogger;
    private static ProgressLogger skippedProgressLogger;

    public static void initialize(long progressInterval, String message) {
        if (progressInterval > 0) {
            progressLogger = new ProgressLogger(progressInterval, message);
        }
    }

    public static void initializeSkipped(long progressInterval, String message) {
        if (progressInterval > 0) {
            skippedProgressLogger = new ProgressLogger(progressInterval, message);
        }
    }

    public static void logProgressIfNecessary(long itemCount) {
        if (progressLogger != null && Util.MAIN_LOGGER.isInfoEnabled()) {
            synchronized (lock) {
                progressLogger.logProgressIfNecessary(itemCount);
            }
        }
    }

    public static void logSkippedProgressIfNecessary(long itemCount) {
        if (skippedProgressLogger != null && Util.MAIN_LOGGER.isInfoEnabled()) {
            synchronized (lock) {
                skippedProgressLogger.logProgressIfNecessary(itemCount);
            }
        }
    }
}
