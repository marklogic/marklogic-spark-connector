/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Extension point for processing documents that have been created based on a Spark row, but before they've been sent
 * to MarkLogic. Intended to shield {@code WriteBatcherDataWriter} from any sort of document processing that occurs,
 * including processing provided by our own connector.
 */
public interface DocumentProcessor extends Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> {
}
