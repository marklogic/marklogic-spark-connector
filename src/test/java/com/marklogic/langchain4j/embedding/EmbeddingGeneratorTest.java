/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.spark.writer.embedding.TestEmbeddingModel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EmbeddingGeneratorTest {

    @Test
    void test() {
        TestEmbeddingModel embeddingModel = new TestEmbeddingModel();
        TestEmbeddingModel.reset();

        EmbeddingGenerator generator = new EmbeddingGenerator(embeddingModel, 3);

        assertFalse(generator.addEmbeddings(makeDocumentWithChunks(2)));
        assertEquals(0, embeddingModel.batchCounter, "No batches should have been sent since only 2 chunks were " +
            "passed in, and the batch size is 3.");
        assertEquals(0, embeddingModel.chunkCounter);

        assertTrue(generator.addEmbeddings(makeDocumentWithChunks(3)));
        assertEquals(2, embeddingModel.batchCounter, "1 batch should have been sent with the 2 pending chunks. A " +
            "second batch is then sent with the other 2 chunks in this document. This is so that we don't have the " +
            "tricky situation of a subset of chunks having been embedded for one document.");
        assertEquals(5, embeddingModel.chunkCounter);

        assertFalse(generator.addEmbeddings(makeDocumentWithChunks(2)));
        assertEquals(2, embeddingModel.batchCounter, "No more batches should have been sent, and both chunks should " +
            "be pending.");
        assertEquals(5, embeddingModel.chunkCounter);

        generator.generateEmbeddingsForPendingChunks();
        assertEquals(3, embeddingModel.batchCounter, "A third batch of 2 chunks should have been sent.");
        assertEquals(7, embeddingModel.chunkCounter);
    }

    private DocumentAndChunks makeDocumentWithChunks(int count) {
        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            chunks.add(new TestEmbeddingModel.TestChunk("text" + i));
        }
        return new DocumentAndChunks(null, chunks);
    }
}
