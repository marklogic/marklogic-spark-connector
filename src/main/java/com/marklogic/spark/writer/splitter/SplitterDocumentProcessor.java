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
import java.util.stream.Stream;

/**
 * Implements the "splitter" capability by delegating to different objects for selecting text to split; splitting
 * the selected text; and then processing the given chunks
 */
public class SplitterDocumentProcessor implements DocumentProcessor {

    private final TextSelector textSelector;
    private final DocumentSplitter documentSplitter;
    private final ChunkAssembler chunkAssembler;

    public SplitterDocumentProcessor(TextSelector textSelector, DocumentSplitter documentSplitter, ChunkAssembler chunkAssembler) {
        this.textSelector = textSelector;
        this.documentSplitter = documentSplitter;
        this.chunkAssembler = chunkAssembler;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        String text = textSelector.selectTextToSplit(sourceDocument);
        if (text == null || text.trim().isEmpty()) {
            return Stream.of(sourceDocument).iterator();
        }
        List<TextSegment> textSegments = documentSplitter.split(new Document(text));
        return chunkAssembler.assembleChunks(sourceDocument, textSegments);
    }
}
