/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;

public interface TextSelector {

    String selectTextToSplit(DocumentWriteOperation operation);
}
