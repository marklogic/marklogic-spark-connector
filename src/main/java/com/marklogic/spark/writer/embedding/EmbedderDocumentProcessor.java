/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.DocumentProcessor;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class EmbedderDocumentProcessor implements DocumentProcessor, Supplier<Iterator<DocumentWriteOperation>> {

    private static final Metadata EMPTY_METADATA = new Metadata();

    private final ChunkSelector chunkSelector;
    private final EmbeddingModel embeddingModel;

    /**
     * This can default to 1, so that each chunk gets sent separately. A user can then bump it up to see how that
     * affects performance and if they run into any rate limit errors.
     */
    private final int batchSize;
    // The chunks extracted from the current batch of documents.
    private List<Chunk> chunks = new ArrayList();
    // The batch of documents to be returned by this processor, after having embeddings added to their chunks.
    private List<DocumentWriteOperation> currentBatch = new ArrayList<>();

    public EmbedderDocumentProcessor(EmbeddingModel embeddingModel, int batchSize) {
        this.chunkSelector = new JsonChunkSelector();
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        if (chunks.size() >= batchSize) {
            currentBatch.clear();
            chunks.clear();
        }
        currentBatch.add(sourceDocument);
        chunkSelector.selectChunks(sourceDocument).forEachRemaining(chunks::add);
        return chunks.size() >= batchSize ? get() : Stream.<DocumentWriteOperation>empty().iterator();
    }

    @Override
    public Iterator<DocumentWriteOperation> get() {
        List<TextSegment> segments = new ArrayList<>();
        chunks.forEach(chunk -> segments.add(new TextSegment(chunk.getEmbeddingText(), EMPTY_METADATA)));
        System.out.println("CALLING: " + segments.size());
        Response<List<Embedding>> response = embeddingModel.embedAll(segments);
        System.out.println("TOKEN USAGE: " + response.tokenUsage());
        for (int i = 0; i < response.content().size(); i++) {
            Chunk chunk = chunks.get(i);
            chunk.addEmbedding(response.content().get(i));
        }
        return currentBatch.iterator();
    }
}
