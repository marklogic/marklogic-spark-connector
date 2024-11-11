/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.Util;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

class EmbeddingGenerator {

    // We don't have any use for metadata, so just need a single instance for constructing text segments.
    private static final Metadata TEXT_SEGMENT_METADATA = new Metadata();

    private final EmbeddingModel embeddingModel;
    private final int batchSize;

    private List<Chunk> pendingChunks = new ArrayList<>();

    EmbeddingGenerator(EmbeddingModel embeddingModel) {
        this(embeddingModel, 1);
    }

    EmbeddingGenerator(EmbeddingModel embeddingModel, int batchSize) {
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    boolean addEmbeddings(DocumentAndChunks documentAndChunks) {
        List<Chunk> chunks = documentAndChunks.getChunks();
        if (chunks == null || chunks.isEmpty()) {
            return false;
        }

        // Iterate over the chunks, flushing as necessary.
        Iterator<Chunk> chunkIterator = chunks.iterator();
        boolean flushOccurred = false;
        while (chunkIterator.hasNext()) {
            addChunkToPendingChunks(chunkIterator.next());
            if (pendingChunks.size() >= batchSize) {
                generateEmbeddingsForPendingChunks();
                flushOccurred = true;
            }
        }

        // If we flushed at least once while iterating, or if we now have enough pending chunks, flush the remaining
        // chunks.
        if (flushOccurred || pendingChunks.size() >= batchSize) {
            generateEmbeddingsForPendingChunks();
            return true;
        }

        return false;
    }

    void generateEmbeddingsForPendingChunks() {
        if (!pendingChunks.isEmpty()) {
            if (Util.EMBEDDER_LOGGER.isDebugEnabled()) {
                Util.EMBEDDER_LOGGER.debug("Generating embeddings for pending chunks; count: {}.", pendingChunks.size());
            }
            addEmbeddingsToChunks(pendingChunks);
            pendingChunks.clear();
        }
    }

    private void addChunkToPendingChunks(Chunk chunk) {
        String text = chunk.getEmbeddingText();
        if (text != null && text.trim().length() > 0) {
            pendingChunks.add(chunk);
        } else if (Util.EMBEDDER_LOGGER.isDebugEnabled()) {
            Util.EMBEDDER_LOGGER.debug("Not generating embedding for chunk in URI {}; could not find text to use for generating an embedding.",
                chunk.getDocumentUri());
        }
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        List<TextSegment> textSegments = chunks.stream()
            .map(chunk -> new TextSegment(chunk.getEmbeddingText(), TEXT_SEGMENT_METADATA))
            .collect(Collectors.toList());

        Response<List<Embedding>> response = embeddingModel.embedAll(textSegments);
        if (Util.EMBEDDER_LOGGER.isInfoEnabled()) {
            Util.EMBEDDER_LOGGER.info("Sent {} chunks; token usage: {}", textSegments.size(), response.tokenUsage());
        }

        List<Embedding> embeddings = response.content();
        for (int i = 0; i < embeddings.size(); i++) {
            chunks.get(i).addEmbedding(embeddings.get(i));
        }
    }
}
