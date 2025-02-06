/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

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
     * @return an iterator, which allows for an implementation to lazily construct documents if necessary.
     */
    Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<String> chunks, List<byte[]> classifications);
}
