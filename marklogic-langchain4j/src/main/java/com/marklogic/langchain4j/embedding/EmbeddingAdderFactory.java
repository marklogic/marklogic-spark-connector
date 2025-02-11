/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.ChunkSelectorFactory;
import dev.langchain4j.model.embedding.EmbeddingModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface EmbeddingAdderFactory {

    static Optional<EmbeddingAdder> makeEmbedder(Context context, DocumentTextSplitter splitter) {
        Optional<EmbeddingModel> embeddingModel = makeEmbeddingModel(context);
        if (embeddingModel.isPresent()) {
            EmbeddingGenerator embeddingGenerator = makeEmbeddingGenerator(context, embeddingModel.get());
            if (splitter != null) {
                return Optional.of(new EmbeddingAdder(splitter, embeddingGenerator));
            }
            ChunkSelector chunkSelector = ChunkSelectorFactory.makeChunkSelector(context);
            return Optional.of(new EmbeddingAdder(chunkSelector, embeddingGenerator));
        }
        return Optional.empty();
    }

    static EmbeddingGenerator makeEmbeddingGenerator(Context context, EmbeddingModel model) {
        int batchSize = context.getIntOption(Options.WRITE_EMBEDDER_BATCH_SIZE, 1, 1);
        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Using embedding model with dimension: {}", model.dimension());
        }
        return new EmbeddingGenerator(model, batchSize);
    }

    static Optional<EmbeddingModel> makeEmbeddingModel(Context context) {
        if (!context.hasOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME)) {
            return Optional.empty();
        }

        final String className = context.getStringOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME);
        try {
            Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
            @SuppressWarnings("unchecked")
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

    private static Map<String, String> makeEmbedderOptions(Context context) {
        Map<String, String> options = new HashMap<>();
        context.getProperties().keySet().stream()
            .filter(key -> key.startsWith(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX))
            .forEach(key -> options.put(key.substring(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX.length()), context.getProperties().get(key)));
        return options;
    }
}
