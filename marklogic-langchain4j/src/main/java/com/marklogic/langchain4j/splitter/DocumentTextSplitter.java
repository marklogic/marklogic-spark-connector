/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.splitter.ChunkAssembler;
import com.marklogic.spark.core.splitter.TextSelector;
import com.marklogic.spark.core.splitter.TextSplitter;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Implements the "splitter" capability by delegating to different objects for selecting text to split; splitting
 * the selected text; and then processing the given chunks
 */
public class DocumentTextSplitter implements Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>, TextSplitter {

    private final TextSelector textSelector;
    private final DocumentSplitter documentSplitter;
    private final ChunkAssembler chunkAssembler;

    public DocumentTextSplitter(TextSelector textSelector, DocumentSplitter documentSplitter, ChunkAssembler chunkAssembler) {
        this.textSelector = textSelector;
        this.documentSplitter = documentSplitter;
        this.chunkAssembler = chunkAssembler;
    }

    @Override
    public List<String> split(String sourceUri, AbstractWriteHandle content) {
        String text = textSelector.selectTextToSplit(content);
        if (text == null || text.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<TextSegment> textSegments;
        try {
            textSegments = documentSplitter.split(new Document(text));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to split document with URI: %s; cause: %s",
                sourceUri, e.getMessage()), e);
        }

        return textSegments.stream().map(TextSegment::text).toList();
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        List<String> segments = split(sourceDocument.getUri(), sourceDocument.getContent());
        if (segments == null || segments.isEmpty()) {
            return Stream.of(sourceDocument).iterator();
        }
        return chunkAssembler.assembleChunks(sourceDocument, segments, null);
    }
}
