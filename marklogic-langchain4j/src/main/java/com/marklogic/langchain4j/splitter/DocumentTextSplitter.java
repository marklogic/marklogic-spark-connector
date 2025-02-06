/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.MarkLogicLangchainException;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Implements the "splitter" capability by delegating to different objects for selecting text to split; splitting
 * the selected text; and then processing the given chunks
 */
public class DocumentTextSplitter implements Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> {

    private final TextSelector textSelector;
    private final DocumentSplitter documentSplitter;
    private final ChunkAssembler chunkAssembler;

    public DocumentTextSplitter(TextSelector textSelector, DocumentSplitter documentSplitter, ChunkAssembler chunkAssembler) {
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

        List<TextSegment> textSegments;
        try {
            textSegments = documentSplitter.split(new Document(text));
        } catch (Exception e) {
            throw new MarkLogicLangchainException(String.format("Unable to split document with URI: %s; cause: %s",
                sourceDocument.getUri(), e.getMessage()), e);
        }

        return chunkAssembler.assembleChunks(sourceDocument, textSegments, null);
    }
}
