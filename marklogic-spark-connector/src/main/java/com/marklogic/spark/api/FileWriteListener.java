/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.api;

/**
 * Callback interface for events that occur while writing documents to files.
 *
 * @since 3.2.0
 */
public interface FileWriteListener {

    /**
     * Called periodically (per the configured progress interval) as URIs are processed, and once more with the
     * final count when the writer commits.
     * <p>
     * {@code totalUriCount} is the cumulative count of URIs processed by this one writer instance (i.e. this one
     * Spark partition/task) -- it is not aggregated across partitions or across separate Spark jobs. A URI counts
     * once regardless of whether it results in one zip entry (content only) or two (content and metadata).
     *
     * @param totalUriCount cumulative number of URIs processed so far by this writer instance
     */
    default void onUrisProcessed(long totalUriCount) {
    }
}
