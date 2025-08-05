/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.langchain4j;

import com.marklogic.langchain4j.embedding.EmbeddingGenerator;
import com.marklogic.langchain4j.splitter.DocumentSplitterFactory;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.embedding.EmbeddingProducerFactory;
import com.marklogic.spark.core.splitter.*;
import com.marklogic.spark.dom.NamespaceContextFactory;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.model.embedding.EmbeddingModel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * This acts as a bridge between the Spark connector and the langchain4j integration that requires Java 17.
 * While the Spark connector and Flux must support Java 11, this should is expected to not be referenced directly
 * but rather instantiated dynamically.
 */
public class Langchain4jFactory implements TextSplitterFactory, EmbeddingProducerFactory {

    public TextSplitter newTextSplitter(Context context) {
        if (context.hasOption(Options.WRITE_SPLITTER_XPATH)) {
            return makeXmlSplitter(context);
        } else if (context.getProperties().containsKey(Options.WRITE_SPLITTER_JSON_POINTERS)) {
            // "" is a valid JSON Pointer expression, so we only check to see if the key exists.
            return makeJsonSplitter(context);
        } else if (context.getBooleanOption(Options.WRITE_SPLITTER_TEXT, false)) {
            return makeTextSplitter(context);
        }
        return null;
    }

    @Override
    public EmbeddingProducer newEmbeddingProducer(Context context) {
        Optional<EmbeddingModel> embeddingModel = makeEmbeddingModel(context);
        return embeddingModel.isPresent() ?
            makeEmbeddingGenerator(context, embeddingModel.get()) :
            null;
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

    private static DocumentTextSplitter makeXmlSplitter(Context context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split XML documents using XPath: {}",
                context.getStringOption(Options.WRITE_SPLITTER_XPATH));
        }
        TextSelector textSelector = makeXmlTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        return new DocumentTextSplitter(textSelector, splitter);
    }

    static TextSelector makeXmlTextSelector(Context context) {
        return makeXmlTextSelector(context.getStringOption(Options.WRITE_SPLITTER_XPATH), context);
    }

    static TextSelector makeXmlTextSelector(String xpath, Context context) {
        return new DOMTextSelector(xpath, NamespaceContextFactory.makeNamespaceContext(context.getProperties()));
    }

    private static DocumentTextSplitter makeJsonSplitter(Context context) {
        TextSelector textSelector = makeJsonTextSelector(context.getProperties().get(Options.WRITE_SPLITTER_JSON_POINTERS));
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        return new DocumentTextSplitter(textSelector, splitter);
    }

    private static TextSelector makeJsonTextSelector(String jsonPointers) {
        String[] pointers = jsonPointers.split("\n");
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split JSON documents using JSON Pointers: {}", Arrays.asList(pointers));
        }
        // Need an option other than "join delimiter", which applies to joining split text, not selected text.
        return new JsonPointerTextSelector(pointers, null);
    }

    private static DocumentTextSplitter makeTextSplitter(Context context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split text documents using all text in each document.");
        }
        return new DocumentTextSplitter(new AllTextSelector(), DocumentSplitterFactory.makeDocumentSplitter(context));
    }
}
