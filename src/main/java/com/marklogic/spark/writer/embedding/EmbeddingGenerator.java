/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.List;

/**
 * Knows how to generate and add embeddings for each chunk. Will soon support a batch size so that more than one
 * chunk can be sent to an embedding model in a single call.
 */
public class EmbeddingGenerator {

    private EmbeddingModel embeddingModel;

    public EmbeddingGenerator(EmbeddingModel embeddingModel) {
        this.embeddingModel = embeddingModel;
    }

    public void addEmbeddings(List<Chunk> chunks) {
        if (chunks != null) {
            chunks.forEach(chunk -> {
                Response<Embedding> response = embeddingModel.embed(chunk.getEmbeddingText());
                chunk.addEmbedding(response.content());
            });
        }
    }
}
