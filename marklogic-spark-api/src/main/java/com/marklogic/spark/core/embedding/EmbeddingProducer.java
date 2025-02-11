/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

// Hides langchain4j, and will eventually support batching.
public interface EmbeddingProducer {

    float[] produceEmbedding(String text);
}
