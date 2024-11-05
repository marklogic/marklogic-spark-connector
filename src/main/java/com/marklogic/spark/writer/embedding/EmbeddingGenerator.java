/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.Util;
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
                if (text != null && text.trim().length() > 0) {
                    Response<Embedding> response = embeddingModel.embed(text);
                    chunk.addEmbedding(response.content());
                } else if (Util.MAIN_LOGGER.isDebugEnabled()) {
                    Util.MAIN_LOGGER.debug("Not generating embedding for chunk in URI {}; could not find text to use for generating an embedding.",
                        chunk.getDocumentUri());
                }
            });
        }
    }
}
