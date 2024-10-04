/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.DocumentProcessor;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;

public class SplitterDocumentProcessor implements DocumentProcessor {

    private final TextSelector textSelector;
    private final DocumentSplitter documentSplitter;
    private final ChunkProcessor chunkProcessor;

    public SplitterDocumentProcessor(TextSelector textSelector, DocumentSplitter documentSplitter, ChunkProcessor chunkProcessor) {
        this.textSelector = textSelector;
        this.documentSplitter = documentSplitter;
        this.chunkProcessor = chunkProcessor;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        String text = textSelector.selectTextToSplit(sourceDocument);
        List<TextSegment> textSegments = documentSplitter.split(new Document(text));
        return chunkProcessor.processChunks(sourceDocument, textSegments);
    }
}
