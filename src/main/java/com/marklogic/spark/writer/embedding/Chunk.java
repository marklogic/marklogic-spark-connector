/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;

public interface Chunk {

    String getEmbeddingText();

    void addEmbedding(Embedding embedding);
}
