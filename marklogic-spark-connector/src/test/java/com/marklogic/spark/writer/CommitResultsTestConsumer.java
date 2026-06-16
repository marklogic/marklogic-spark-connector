/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.api.WriteListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only consumer for capturing commit results. Tests can access the static fields to verify counts.
 */
public class CommitResultsTestConsumer implements WriteListener {

    public static final AtomicLong successCount = new AtomicLong(0);
    public static final AtomicLong failureCount = new AtomicLong(0);
    public static final AtomicLong skippedCount = new AtomicLong(0);
    public static final List<Long> loggedSuccessCounts = new ArrayList<>();
    public static final List<Long> loggedSkippedCounts = new ArrayList<>();
    public static Map<String, String> failedDocuments;

    public static Map<String, String> params;

    public CommitResultsTestConsumer(Map<String, String> params) {
        CommitResultsTestConsumer.params = params;
    }

    @Override
    public void onSuccessCountLogged(long itemCount) {
        loggedSuccessCounts.add(itemCount);
    }

    @Override
    public void onSkippedCountLogged(long itemCount) {
        loggedSkippedCounts.add(itemCount);
    }

    @Override
    public void onWriteCommit(CommitResults commitResults) {
        successCount.set(commitResults.getSuccessCount());
        failureCount.set(commitResults.getFailureCount());
        skippedCount.set(commitResults.getSkippedCount());
        failedDocuments = commitResults.getFailedDocuments();
    }

    public static void reset() {
        successCount.set(0);
        failureCount.set(0);
        skippedCount.set(0);
        loggedSuccessCounts.clear();
        loggedSkippedCounts.clear();
        params = null;
        failedDocuments = null;
    }
}
