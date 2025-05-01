/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.ConnectorException;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;

import java.util.Map;
import java.util.function.Function;

public class MinilmEmbeddingModelFunction implements Function<Map<String, String>, EmbeddingModel> {

    @Override
    public EmbeddingModel apply(Map<String, String> options) {
        if ("true".equals(options.get("throwError"))) {
            throw new ConnectorException("Intentional error.");
        }
        return new AllMiniLmL6V2EmbeddingModel();
    }
}
