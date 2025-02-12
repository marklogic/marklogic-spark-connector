/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.spark.core.DocumentInputs;

import java.util.List;

public interface EmbeddingProducer {

    /**
     * @param inputs
     * @return zero or more input objects with embeddings added to the chunks. The count depends on whether batching
     * is enabled in the implementation.
     */
    List<DocumentInputs> produceEmbeddings(DocumentInputs inputs);

    /**
     * @return zero or more input objects with embeddings added to the chunks. Intended to catch any leftover input
     * objects such that the batch size wasn't met.
     */
    List<DocumentInputs> flush();

}
