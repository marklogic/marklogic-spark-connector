/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.api;

import com.marklogic.client.datamovement.WriteBatch;

import java.util.Map;
import java.util.Set;

/**
 * Callback interface for various events that occur during the write process.
 *
 * @since 3.2.0
 */
public interface WriteListener {

    default void onBatchSuccess(WriteBatch batch) {
    }

    default void onSuccessCountLogged(long itemCount) {
    }

    default void onSkippedCountLogged(long itemCount) {
    }

    default void onWriteCommit(CommitResults commitResults) {
    }

    class CommitResults {

        private final long successCount;
        private final long skippedCount;
        private final long failureCount;
        private final Set<String> graphs;
        private final Map<String, String> failedDocuments;

        public CommitResults(long successCount, long skippedCount, long failureCount, Set<String> graphs, Map<String, String> failedDocuments) {
            this.successCount = successCount;
            this.skippedCount = skippedCount;
            this.failureCount = failureCount;
            this.graphs = graphs;
            this.failedDocuments = failedDocuments;
        }

        public long getSuccessCount() {
            return successCount;
        }

        public long getSkippedCount() {
            return skippedCount;
        }

        public long getFailureCount() {
            return failureCount;
        }

        public Map<String, String> getFailedDocuments() {
            return failedDocuments;
        }

        public Set<String> getGraphs() {
            return graphs;
        }
    }
}
