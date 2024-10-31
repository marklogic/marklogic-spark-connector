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
                String text = chunk.getEmbeddingText();
                // Not yet clear if an error should be thrown here or not. It may be okay if most documents in a query
                // adhere to an expected structure, but some do not - i.e. if some documents have chunks that do not
                // have their text in the same place.
                if (text != null && text.trim().length() > 0) {
                    Response<Embedding> response = embeddingModel.embed(text);
                    chunk.addEmbedding(response.content());
                }
            });
        }
    }
}
