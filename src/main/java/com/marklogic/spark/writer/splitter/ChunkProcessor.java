/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;

public interface ChunkProcessor {

    /**
     * @param sourceDocument
     * @param chunks
     * @return TODO The benefit of an iterator would be in lazily building chunks, when generating each chunk could be
     * expensive - such as when including an embedding in the chunk. A user may not want to be forced into building a
     * List right away.
     */
    Iterator<DocumentWriteOperation> processChunks(
        DocumentWriteOperation sourceDocument,
        List<TextSegment> chunks
    );
}
