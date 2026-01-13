/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.Iterator;
import java.util.List;

/**
 * Defines how chunks are assembled into one or more documents to be written to MarkLogic.
 */
public interface ChunkAssembler {

    /**
     * @param sourceDocument
     * @param chunks
     * @param classifications
     * @param embeddings
     * @return an iterator, which allows for an implementation to lazily construct documents if necessary.
     */
    Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<String> chunks,
                                                    List<byte[]> classifications, List<float[]> embeddings);
}
