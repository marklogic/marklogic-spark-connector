/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;

/**
 * Defines how text to be split is selected from a document.
 */
public interface TextSelector {

    String selectTextToSplit(DocumentWriteOperation sourceDocument);
}
