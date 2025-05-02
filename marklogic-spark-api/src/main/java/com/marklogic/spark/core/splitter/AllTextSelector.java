/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.marker.AbstractWriteHandle;

/**
 * Intended for text documents and JSON/XML documents where the entire document should be used, including
 * the structure.
 */
public class AllTextSelector implements TextSelector {

    @Override
    public String selectTextToSplit(DocumentWriteOperation operation) {
        return selectTextToSplit(operation.getContent());
    }

    @Override
    public String selectTextToSplit(AbstractWriteHandle contentHandle) {
        return HandleAccessor.contentAsString(contentHandle);
    }
}
