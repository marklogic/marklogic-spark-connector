/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles the progress counter for any operation involving reading from MarkLogic. A Spark job/application can only have
 * one reader, and thus DefaultSource handles resetting this counter before a new read job starts up. A static counter
 * is used so that all reader partitions in the same JVM can have their progress aggregated and logged.
 */
public class ReadProgressLogger implements Serializable {

    static final long serialVersionUID = 1L;

    private static final AtomicLong progressCounter = new AtomicLong(0);
    private static long progressInterval;
    private static long nextProgressInterval;
    private static String message;

    public static void initialize(long progressInterval, String message) {
        progressCounter.set(0);
        ReadProgressLogger.progressInterval = progressInterval;
        nextProgressInterval = progressInterval;
        ReadProgressLogger.message = message;
    }

    public static void logProgressIfNecessary(long itemCount) {
        if (progressInterval > 0 && progressCounter.addAndGet(itemCount) >= nextProgressInterval) {
            synchronized (progressCounter) {
                Util.MAIN_LOGGER.info(message, nextProgressInterval);
                nextProgressInterval += progressInterval;
            }
        }
    }
}
