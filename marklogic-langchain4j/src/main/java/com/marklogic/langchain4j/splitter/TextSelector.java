/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;

/**
 * Defines how text to be split is selected from a document.
 */
public interface TextSelector {

    String selectTextToSplit(DocumentWriteOperation sourceDocument);

    default String selectTextToSplit(AbstractWriteHandle contentHandle) {
        throw new UnsupportedOperationException();
    }
}
