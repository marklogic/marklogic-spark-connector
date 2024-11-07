/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EmbeddingGeneratorTest {

    @Test
    void test() {
        TestEmbeddingModel embeddingModel = new TestEmbeddingModel();
        TestEmbeddingModel.batchCounter = 0;

        EmbeddingGenerator generator = new EmbeddingGenerator(embeddingModel, 2);

        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            chunks.add(new TestEmbeddingModel.TestChunk("text" + i));
        }

        generator.addEmbeddings(chunks);
        assertEquals(3, embeddingModel.batchCounter, "3 batches should have been sent given the batch size of 2.");
    }

}
