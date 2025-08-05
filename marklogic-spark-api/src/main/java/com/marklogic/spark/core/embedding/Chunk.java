/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

/**
 * Represents a chunk in either a JSON or XML document.
 */
public interface Chunk {

    /**
     * @return the URI of the document containing this chunk
     */
    String getDocumentUri();

    /**
     * @return the text to be used for generating an embedding.
     */
    String getEmbeddingText();

    /**
     * Add the vector data in the given embedding to the chunk.
     *
     * @param embedding
     */
    void addEmbedding(float[] embedding);
}
