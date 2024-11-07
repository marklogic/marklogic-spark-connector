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

/**
 * Knows how to generate and add embeddings for each chunk. Will soon support a batch size so that more than one
 * chunk can be sent to an embedding model in a single call.
 */
public class EmbeddingGenerator {

    // We don't have any use for metadata, so just need a single instance for constructing text segments.
    private static final Metadata TEXT_SEGMENT_METADATA = new Metadata();

    private final EmbeddingModel embeddingModel;
    private final int batchSize;

    public EmbeddingGenerator(EmbeddingModel embeddingModel) {
        this(embeddingModel, 1);
    }

    public EmbeddingGenerator(EmbeddingModel embeddingModel, int batchSize) {
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    public void addEmbeddings(List<Chunk> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return;
        }

        Iterator<Chunk> chunkIterator = chunks.iterator();
        List<Chunk> batch = new ArrayList<>();
        while (chunkIterator.hasNext()) {
            Chunk chunk = chunkIterator.next();
            String text = chunk.getEmbeddingText();
            if (text != null && text.trim().length() > 0) {
                batch.add(chunk);
                if (batch.size() >= this.batchSize) {
                    addEmbeddingsToChunks(batch);
                    batch = new ArrayList<>();
                }
            } else if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Not generating embedding for chunk in URI {}; could not find text to use for generating an embedding.",
                    chunk.getDocumentUri());
            }
        }

        if (!batch.isEmpty()) {
            addEmbeddingsToChunks(batch);
        }
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        List<TextSegment> textSegments = chunks.stream()
            .map(chunk -> new TextSegment(chunk.getEmbeddingText(), TEXT_SEGMENT_METADATA))
            .collect(Collectors.toList());

        Response<List<Embedding>> response = embeddingModel.embedAll(textSegments);
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Sent {} chunks; token usage: {}", textSegments.size(), response.tokenUsage());
        }

        List<Embedding> embeddings = response.content();
        for (int i = 0; i < embeddings.size(); i++) {
            chunks.get(i).addEmbedding(embeddings.get(i));
        }
    }
}
