/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import java.util.List;

public interface EmbeddingProducer {

    void addEmbeddings(List<Chunk> chunks);

}
