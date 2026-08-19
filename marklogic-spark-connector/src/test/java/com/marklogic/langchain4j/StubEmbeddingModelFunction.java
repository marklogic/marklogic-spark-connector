/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.langchain4j;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Minimal stub used in unit tests to verify that a valid Function class is accepted without errors.
 * Does not load any native ONNX or DJL resources.
 */
public class StubEmbeddingModelFunction implements Function<Map<String, String>, EmbeddingModel>, EmbeddingModel {

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
        return Response.from(List.of());
    }
}
