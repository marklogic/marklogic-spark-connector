/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;

/**
 * Defines how chunks are assembled into one or more documents to be written to MarkLogic.
 */
public interface ChunkAssembler {

    /**
     * @param sourceDocument
     * @param chunks
     * @return an iterator, which allows for an implementation to lazily construct documents if necessary.
     */
    Iterator<DocumentWriteOperation> assembleChunks(
        DocumentWriteOperation sourceDocument,
        List<TextSegment> chunks
    );

    // This will eventually be the preferred method, we'll get rid of the one that exposes the LangChain4j type.
    Iterator<DocumentWriteOperation> assembleStringChunks(DocumentWriteOperation sourceDocument, List<String> chunks);
}
