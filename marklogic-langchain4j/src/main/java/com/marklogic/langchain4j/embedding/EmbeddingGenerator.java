/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddingGenerator implements EmbeddingProducer {

    // We don't have any use for metadata, so just need a single instance for constructing text segments.
    private static final Metadata TEXT_SEGMENT_METADATA = new Metadata();

    private final EmbeddingModel embeddingModel;
    private final int batchSize;

    // Only used for debug logging.
    private static final AtomicLong tokenCount = new AtomicLong(0);
    private static final AtomicLong requestCount = new AtomicLong(0);

    private List<Chunk> pendingChunks = new ArrayList<>();

    public EmbeddingGenerator(EmbeddingModel embeddingModel) {
        this(embeddingModel, 1);
    }

    public EmbeddingGenerator(EmbeddingModel embeddingModel, int batchSize) {
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    @Override
    public float[] produceEmbedding(String text) {
        // No batching support yet. Next PR.
        return embeddingModel.embed(text).content().vector();
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
            if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isDebugEnabled()) {
                EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Generating embeddings for pending chunks; count: {}.", pendingChunks.size());
            }
            addEmbeddingsToChunks(pendingChunks);
            pendingChunks.clear();
        }
    }

    private void addChunkToPendingChunks(Chunk chunk) {
        String text = chunk.getEmbeddingText();
        if (text != null && text.trim().length() > 0) {
            pendingChunks.add(chunk);
        } else if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isDebugEnabled()) {
            EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Not generating embedding for chunk in URI {}; could not find text to use for generating an embedding.",
                chunk.getDocumentUri());
        }
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        List<TextSegment> textSegments = makeTextSegments(chunks);
        Response<List<Embedding>> response = embeddingModel.embedAll(textSegments);
        logResponse(response, textSegments);

        if (response.content() == null) {
            EmbeddingUtil.LANGCHAIN4J_LOGGER.warn("Sent {} chunks; no embeddings were returned; finish reason: {}",
                textSegments.size(), response.finishReason());
        } else {
            List<Embedding> embeddings = response.content();
            for (int i = 0; i < embeddings.size(); i++) {
                chunks.get(i).addEmbedding(embeddings.get(i).vector());
            }
        }
    }

    private List<TextSegment> makeTextSegments(List<Chunk> chunks) {
        return chunks.stream()
            .map(chunk -> new TextSegment(chunk.getEmbeddingText(), TEXT_SEGMENT_METADATA))
            .toList();
    }

    private void logResponse(Response<List<Embedding>> response, List<TextSegment> textSegments) {
        if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isInfoEnabled()) {
            // Not every embedding model provides token usage.
            if (response.tokenUsage() != null) {
                EmbeddingUtil.LANGCHAIN4J_LOGGER.info("Sent {} chunks; token usage: {}", textSegments.size(), response.tokenUsage());
            } else {
                EmbeddingUtil.LANGCHAIN4J_LOGGER.info("Sent {} chunks", textSegments.size());
            }

            if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isDebugEnabled()) {
                long totalRequests = requestCount.incrementAndGet();
                if (response.tokenUsage() != null) {
                    EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Requests: {}; tokens: {}", totalRequests,
                        tokenCount.addAndGet(response.tokenUsage().inputTokenCount())
                    );
                } else {
                    EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Requests: {}", totalRequests);
                }
            }
        }
    }
}
