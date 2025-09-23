/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import java.util.List;

public interface EmbeddingProducer {

    void addEmbeddings(List<Chunk> chunks);

}
