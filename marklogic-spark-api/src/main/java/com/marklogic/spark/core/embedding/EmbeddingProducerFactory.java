/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.spark.Context;

public interface EmbeddingProducerFactory {

    EmbeddingProducer newEmbeddingProducer(Context context);
}
