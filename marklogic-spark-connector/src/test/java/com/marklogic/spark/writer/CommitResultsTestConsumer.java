/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Test-only consumer for capturing commit results. Tests can access the static fields to verify counts.
 */
public class CommitResultsTestConsumer implements Consumer<Map<String, Object>> {

    public static final AtomicLong successCount = new AtomicLong(0);
    public static final AtomicLong failureCount = new AtomicLong(0);
    public static final AtomicLong skippedCount = new AtomicLong(0);

    public static Map<String, String> params;

    public CommitResultsTestConsumer(Map<String, String> params) {
        CommitResultsTestConsumer.params = params;
    }

    @Override
    public void accept(Map<String, Object> results) {
        successCount.set((Long) results.get("successCount"));
        failureCount.set((Long) results.get("failureCount"));
        skippedCount.set((Long) results.get("skippedCount"));
    }

    public static void reset() {
        successCount.set(0);
        failureCount.set(0);
        skippedCount.set(0);
        params = null;
    }
}
