/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
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
     * For splitting text via a UDF. Unfortunately not able to include a URI in error messages from this, as the UDF
     * only has access to the column value.
     */
    default String selectTextToSplit(AbstractWriteHandle contentHandle) {
        throw new UnsupportedOperationException();
    }
}
