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
            ChunkSelector chunkSelector = makeChunkSelector(context);
            return Optional.of(new EmbedderDocumentProcessor(chunkSelector, embeddingModel.get()));
        }
        return Optional.empty();
    }

    private static ChunkSelector makeChunkSelector(ContextSupport context) {
        return new JsonChunkSelector.Builder()
            .withChunksPointer(context.getProperties().get(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER))
            .withTextPointer(context.getStringOption(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER))
            .withEmbeddingArrayName(context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAME))
            .build();
    }

    public static Optional<EmbeddingModel> makeEmbeddingModel(ContextSupport context) {
        if (!context.hasOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME)) {
            return Optional.empty();
        }

        final String className = context.getStringOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME);
        try {
            Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
            Function<Map<String, String>, EmbeddingModel> modelFunction = (Function<Map<String, String>, EmbeddingModel>) instance;
            Map<String, String> embedderOptions = makeEmbedderOptions(context);
            return Optional.of(modelFunction.apply(embedderOptions));
        } catch (Exception ex) {
            String message = ex.getMessage();
            if (ex instanceof ClassNotFoundException) {
                message = "Could not load class " + className;
            }
            throw new ConnectorException(String.format("Unable to instantiate class for creating an embedding model; " +
                "class name: %s; cause: %s", className, message), ex);
        }
    }

    private static Map<String, String> makeEmbedderOptions(ContextSupport context) {
        Map<String, String> options = new HashMap<>();
        context.getProperties().keySet().stream()
            .filter(key -> key.startsWith(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX))
            .forEach(key -> options.put(key.substring(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX.length()), context.getProperties().get(key)));
        return options;
    }

    private EmbedderDocumentProcessorFactory() {
    }
}
