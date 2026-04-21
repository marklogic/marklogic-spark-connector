/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Test-only consumer for capturing commit results. Tests can access the static fields to verify counts.
 */
public class CommitResultsTestConsumer implements Consumer<Map<String, Object>> {

    public static final AtomicInteger successCount = new AtomicInteger(0);
    public static final AtomicInteger failureCount = new AtomicInteger(0);
    public static final AtomicInteger skippedCount = new AtomicInteger(0);

    @Override
    public void accept(Map<String, Object> results) {
        successCount.set((Integer) results.get("successCount"));
        failureCount.set((Integer) results.get("failureCount"));
        skippedCount.set((Integer) results.get("skippedCount"));
    }

    public static void reset() {
        successCount.set(0);
        failureCount.set(0);
        skippedCount.set(0);
    }
}
