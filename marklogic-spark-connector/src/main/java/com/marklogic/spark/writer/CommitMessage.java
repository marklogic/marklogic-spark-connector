/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Set;

/**
 * @param graphs           zero or more MarkLogic Semantics graph names, each of which is associated with a
 *                         graph document in MarkLogic that must be created after all the documents have been written.
 * @param skippedItemCount number of documents skipped by an incremental write filter
 */
public record CommitMessage(
    int successItemCount,
    int failedItemCount,
    int skippedItemCount,
    Set<String> graphs
) implements WriterCommitMessage {
}
