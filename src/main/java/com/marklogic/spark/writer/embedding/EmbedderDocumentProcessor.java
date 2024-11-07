/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.DocumentProcessor;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Supports a use case where a document already has chunks in it, which must be selected via a {@code ChunkSelector}.
 * The {@code EmbeddingModel} is then used to generate and add an embedding to each chunk in a given document.
 */
class EmbedderDocumentProcessor implements DocumentProcessor {

    private final ChunkSelector chunkSelector;
    private final EmbeddingGenerator embeddingGenerator;

    EmbedderDocumentProcessor(ChunkSelector chunkSelector, EmbeddingGenerator embeddingGenerator) {
        this.chunkSelector = chunkSelector;
        this.embeddingGenerator = embeddingGenerator;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        ChunkSelector.DocumentAndChunks documentAndChunks = chunkSelector.selectChunks(sourceDocument);
        if (documentAndChunks.getChunks() != null && !documentAndChunks.getChunks().isEmpty()) {
            embeddingGenerator.addEmbeddings(documentAndChunks.getChunks());
        }
        return Stream.of(documentAndChunks.getDocumentToWrite()).iterator();
    }
}
