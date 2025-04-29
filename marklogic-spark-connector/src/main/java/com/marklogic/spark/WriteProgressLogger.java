/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
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
