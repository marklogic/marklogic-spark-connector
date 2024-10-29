/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;

/**
 * Represents a chunk in either a JSON or XML document.
 */
public interface Chunk {

    /**
     * @return the text to be used for generating an embedding.
     */
    String getEmbeddingText();

    /**
     * Add the vector data in the given embedding to the chunk.
     *
     * @param embedding
     */
    void addEmbedding(Embedding embedding);
}
