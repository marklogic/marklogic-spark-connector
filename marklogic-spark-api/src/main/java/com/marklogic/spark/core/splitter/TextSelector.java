/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;

/**
 * Defines how text to be split is selected from a document.
 */
public interface TextSelector {

    String selectTextToSplit(DocumentWriteOperation sourceDocument);

    /**
     * Will add the URI to this soon, now that we're not using a UDF.
     */
    default String selectTextToSplit(AbstractWriteHandle contentHandle) {
        throw new UnsupportedOperationException();
    }
}
