/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.DocumentProcessor;
import dev.langchain4j.model.embedding.EmbeddingModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class EmbedderDocumentProcessorFactory {

    public static Optional<DocumentProcessor> makeEmbedder(ContextSupport context) {
        Optional<EmbeddingModel> embeddingModel = makeEmbeddingModel(context);
        if (embeddingModel.isPresent()) {
            // Will make this configurable soon.
            ChunkSelector chunkSelector = new JsonChunkSelector();
            return Optional.of(new EmbedderDocumentProcessor(chunkSelector, embeddingModel.get()));
        }
        return Optional.empty();
    }

    public static Optional<EmbeddingModel> makeEmbeddingModel(ContextSupport context) {
        if (!context.hasOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME)) {
            return Optional.empty();
        }

        final String className = context.getStringOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME);

        try {
            Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
            Function<Map<String, String>, EmbeddingModel> modelFunction = (Function<Map<String, String>, EmbeddingModel>) instance;
            // Will add options once we support Azure.
            return Optional.of(modelFunction.apply(new HashMap<>()));
        } catch (Exception ex) {
            String message = ex.getMessage();
            if (ex instanceof ClassNotFoundException) {
                message = "Could not load class " + className;
            }
            throw new ConnectorException(String.format("Unable to instantiate class for creating an embedding model; " +
                "class name: %s; cause: %s", className, message), ex);
        }
    }

    private EmbedderDocumentProcessorFactory() {
    }
}
