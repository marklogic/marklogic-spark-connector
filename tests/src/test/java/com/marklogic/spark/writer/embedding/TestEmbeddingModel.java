/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.core.embedding.Chunk;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Used for testing the embedder batch size feature.
 */
public class TestEmbeddingModel implements EmbeddingModel, Function<Map<String, String>, EmbeddingModel> {

    public static int batchCounter;
    public static int chunkCounter;
    public static List<String> chunkTexts = new ArrayList<>();
    public static List<Integer> batchSizes = new ArrayList<>();
    public static boolean useFixedTestVector = false;

    public static void reset() {
        batchCounter = 0;
        chunkCounter = 0;
        chunkTexts.clear();
        batchSizes.clear();
        useFixedTestVector = false;
    }

    private static AllMiniLmL6V2EmbeddingModel realEmbeddingModel = new AllMiniLmL6V2EmbeddingModel();

    @Override
    public EmbeddingModel apply(Map<String, String> options) {
        return this;
    }

    @Override
    public int dimension() {
        return 0;
    }

    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
        batchCounter++;
        batchSizes.add(textSegments.size());
        chunkCounter += textSegments.size();
        textSegments.forEach(segment -> chunkTexts.add(segment.text()));

        if (useFixedTestVector) {
            // Return the fixed test vector float[]{3.14f, 1.59f, 2.65f} for each text segment
            List<Embedding> embeddings = new ArrayList<>();
            for (int i = 0; i < textSegments.size(); i++) {
                embeddings.add(Embedding.from(new float[]{3.14f, 1.59f, 2.65f}));
            }
            return Response.from(embeddings);
        } else {
            return realEmbeddingModel.embedAll(textSegments);
        }
    }

    public static class TestChunk implements Chunk {

        private final String text;

        public TestChunk(String text) {
            this.text = text;
        }

        @Override
        public String getDocumentUri() {
            return "/doesnt/matter.json";
        }

        @Override
        public String getEmbeddingText() {
            return text;
        }

        @Override
        public void addEmbedding(float[] embedding) {
            // Don't need to do this for the purposes of our test.
        }
    }
}
