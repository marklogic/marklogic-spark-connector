/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
