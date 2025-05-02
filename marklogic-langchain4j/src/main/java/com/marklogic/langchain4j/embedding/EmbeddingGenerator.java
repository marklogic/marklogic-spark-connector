/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.spark.Util;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddingGenerator implements EmbeddingProducer {

    private static final Logger LANGCHAIN4J_LOGGER = LoggerFactory.getLogger("com.marklogic.langchain4j");

    // We don't have any use for metadata, so just need a single instance for constructing text segments.
    private static final Metadata TEXT_SEGMENT_METADATA = new Metadata();

    private final EmbeddingModel embeddingModel;
    private final int batchSize;

    // Only used for debug logging.
    private static final AtomicLong tokenCount = new AtomicLong(0);
    private static final AtomicLong requestCount = new AtomicLong(0);

    public EmbeddingGenerator(EmbeddingModel embeddingModel, int batchSize) {
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    @Override
    public void addEmbeddings(List<Chunk> chunks) {
        List<TextSegment> segments = new ArrayList<>();
        int chunkCounter = 0;
        for (Chunk chunk : chunks) {
            segments.add(new TextSegment(chunk.getEmbeddingText(), TEXT_SEGMENT_METADATA));
            if (segments.size() >= batchSize) {
                chunkCounter = generateAndAddEmbeddings(segments, chunks, chunkCounter);
                segments.clear();
            }
        }

        if (!segments.isEmpty()) {
            generateAndAddEmbeddings(segments, chunks, chunkCounter);
        }
    }

    private int generateAndAddEmbeddings(List<TextSegment> segments, List<Chunk> chunks, int chunkCounter) {
        List<Embedding> embeddings = generateEmbeddings(segments);
        for (int i = 0; i < embeddings.size(); i++) {
            Embedding embedding = embeddings.get(i);
            chunks.get(chunkCounter).addEmbedding(embedding.vector());
            chunkCounter++;
        }
        return chunkCounter;
    }

    private List<Embedding> generateEmbeddings(List<TextSegment> textSegments) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Embedding segments, count: {}", textSegments.size());
        }
        Response<List<Embedding>> response = embeddingModel.embedAll(textSegments);
        logResponse(response, textSegments);
        return response.content();
    }

    private void logResponse(Response<List<Embedding>> response, List<TextSegment> textSegments) {
        if (LANGCHAIN4J_LOGGER.isInfoEnabled()) {
            // Not every embedding model provides token usage.
            if (response.tokenUsage() != null) {
                LANGCHAIN4J_LOGGER.info("Sent {} chunks; token usage: {}", textSegments.size(), response.tokenUsage());
            } else {
                LANGCHAIN4J_LOGGER.info("Sent {} chunks", textSegments.size());
            }

            if (LANGCHAIN4J_LOGGER.isDebugEnabled()) {
                long totalRequests = requestCount.incrementAndGet();
                if (response.tokenUsage() != null) {
                    LANGCHAIN4J_LOGGER.debug("Requests: {}; tokens: {}", totalRequests,
                        tokenCount.addAndGet(response.tokenUsage().inputTokenCount())
                    );
                } else {
                    LANGCHAIN4J_LOGGER.debug("Requests: {}", totalRequests);
                }
            }
        }
    }
}
