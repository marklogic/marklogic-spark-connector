/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.core.ChunkInputs;

import java.util.Iterator;
import java.util.List;

/**
 * Defines how chunks are assembled into one or more documents to be written to MarkLogic.
 */
public interface ChunkAssembler {

    /**
     * @param sourceDocument
     * @param chunkInputsList
     * @return an iterator, which allows for an implementation to lazily construct documents if necessary.
     */
    Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<ChunkInputs> chunkInputsList);
}
