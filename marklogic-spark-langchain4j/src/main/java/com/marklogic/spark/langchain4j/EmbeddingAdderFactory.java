/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.langchain4j.embedding.*;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import dev.langchain4j.model.embedding.EmbeddingModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class EmbeddingAdderFactory {

    public static Optional<EmbeddingAdder> makeEmbedder(Context context, DocumentTextSplitter splitter) {
        Optional<EmbeddingModel> embeddingModel = makeEmbeddingModel(context);
        if (embeddingModel.isPresent()) {
            EmbeddingGenerator embeddingGenerator = makeEmbeddingGenerator(context, embeddingModel.get());
            if (splitter != null) {
                return Optional.of(new EmbeddingAdder(splitter, embeddingGenerator));
            }
            ChunkSelector chunkSelector = makeChunkSelector(context);
            return Optional.of(new EmbeddingAdder(chunkSelector, embeddingGenerator));
        }
        return Optional.empty();
    }

    private static EmbeddingGenerator makeEmbeddingGenerator(Context context, EmbeddingModel model) {
        int batchSize = context.getIntOption(Options.WRITE_EMBEDDER_BATCH_SIZE, 1, 1);
        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Using embedding model with dimension: {}", model.dimension());
        }
        return new EmbeddingGenerator(model, batchSize);
    }

    /**
     * If the user is also splitting the documents, then we'll know the location of the chunks based on the default
     * chunks data structure produced by the splitter. If the user is instead processing documents that already have
     * chunks in them from a previous process, then the user needs to tell the connector where to find those chunks -
     * either via a JSON Pointer or an XPath expression.
     *
     * @param context
     * @return
     */
    private static ChunkSelector makeChunkSelector(Context context) {
        if (context.hasOption(Options.WRITE_SPLITTER_JSON_POINTERS)) {
            return makeJsonChunkSelector(context);
        } else if (context.hasOption(Options.WRITE_SPLITTER_XPATH)) {
            return makeXmlChunkSelector(context);
        } else if (context.getProperties().get(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER) != null) {
            // "" is allowed for the chunks JSON pointer.
            return makeJsonChunkSelector(context);
        } else if (context.hasOption(Options.WRITE_EMBEDDER_CHUNKS_XPATH)) {
            return makeXmlChunkSelector(context);
        } else if (context.hasOption(Options.WRITE_SPLITTER_TEXT)) {
            return "xml".equalsIgnoreCase(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE)) ?
                makeXmlChunkSelector(context) :
                makeJsonChunkSelector(context);
        }
        throw new ConnectorException(String.format("To generate embeddings on documents, you must specify either " +
                "%s or %s to define the location of chunks in documents.",
            context.getOptionNameForMessage(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER),
            context.getOptionNameForMessage(Options.WRITE_EMBEDDER_CHUNKS_XPATH)
        ));
    }

    private static ChunkSelector makeJsonChunkSelector(Context context) {
        return new JsonChunkSelector.Builder()
            .withChunksPointer(context.getProperties().get(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER))
            .withTextPointer(context.getStringOption(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER))
            .withEmbeddingArrayName(context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAME))
            .build();
    }

    private static ChunkSelector makeXmlChunkSelector(Context context) {
        XmlChunkConfig xmlChunkConfig = new XmlChunkConfig(
            context.getStringOption(Options.WRITE_EMBEDDER_TEXT_XPATH),
            context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAME),
            context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE),
            NamespaceContextFactory.makeNamespaceContext(context.getProperties())
        );
        return new DOMChunkSelector(
            context.getStringOption(Options.WRITE_EMBEDDER_CHUNKS_XPATH),
            xmlChunkConfig
        );
    }

    private static Optional<EmbeddingModel> makeEmbeddingModel(Context context) {
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

    private static Map<String, String> makeEmbedderOptions(Context context) {
        Map<String, String> options = new HashMap<>();
        context.getProperties().keySet().stream()
            .filter(key -> key.startsWith(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX))
            .forEach(key -> options.put(key.substring(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX.length()), context.getProperties().get(key)));
        return options;
    }

    private EmbeddingAdderFactory() {
    }
}
