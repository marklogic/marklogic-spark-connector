/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Used for testing the embedder batch size feature.
 */
class TestEmbeddingModel implements EmbeddingModel, Function<Map<String, String>, EmbeddingModel> {

    static int batchCounter;

    @Override
    public EmbeddingModel apply(Map<String, String> options) {
        return this;
    }

    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
        batchCounter++;
        return Response.from(Arrays.asList(new Embedding(new float[]{1})));
    }

    static class TestChunk implements Chunk {

        private final String text;

        TestChunk(String text) {
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
        public void addEmbedding(Embedding embedding) {
            // Don't need to do this for the purposes of our test.
        }
    }
}
