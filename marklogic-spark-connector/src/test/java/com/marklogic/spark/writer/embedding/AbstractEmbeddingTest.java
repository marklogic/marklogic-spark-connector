/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;

abstract class AbstractEmbeddingTest extends AbstractIntegrationTest {

    static final String TEST_EMBEDDING_FUNCTION_CLASS = "com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction";

    // The minilm embedding model returns "unknown" as its model name.
    static final String EXPECTED_MODEL_NAME = "unknown";

    @AfterEach
    void teardown() {
        TestEmbeddingModel.reset();
    }
}
