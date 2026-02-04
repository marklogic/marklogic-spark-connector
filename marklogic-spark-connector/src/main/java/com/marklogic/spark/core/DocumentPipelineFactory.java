/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.langchain4j.Langchain4jFactory;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.classifier.TextClassifierFactory;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.ChunkSelectorFactory;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.embedding.EmbeddingProducerFactory;
import com.marklogic.spark.core.extraction.TextExtractor;
import com.marklogic.spark.core.extraction.TikaTextExtractor;
import com.marklogic.spark.core.nuclia.DefaultNuaClient;
import com.marklogic.spark.core.nuclia.MockNuaClient;
import com.marklogic.spark.core.nuclia.NuaClient;
import com.marklogic.spark.core.splitter.TextSplitter;
import com.marklogic.spark.core.splitter.TextSplitterFactory;

public abstract class DocumentPipelineFactory {

    private static final String MOCK_NUA_CLIENT_OPTION = "spark.marklogic.testing.mockNuaClientResponse";

    // For some reason, Sonar thinks the check for four nulls always resolves to false, even though it's definitely
    // possible. So ignoring that warning.
    @SuppressWarnings("java:S2589")
    public static DocumentPipeline newDocumentPipeline(Context context) {
        // Check for Nuclia configuration first
        NuaClient nuaClient = newNuaClient(context);
        if (nuaClient != null) {
            TextClassifier textClassifier = TextClassifierFactory.newTextClassifier(context);
            return new DocumentPipeline(nuaClient, textClassifier);
        }

        // Standard pipeline with separate components
        final TextExtractor textExtractor = context.getBooleanOption(Options.WRITE_EXTRACTED_TEXT, false) ?
            new TikaTextExtractor() : null;
        final TextSplitter textSplitter = newTextSplitter(context);
        final TextClassifier textClassifier = TextClassifierFactory.newTextClassifier(context);
        final EmbeddingProducer embeddingProducer = newEmbeddingProducer(context);

        ChunkSelector chunkSelector = null;
        if (embeddingProducer != null && textSplitter == null) {
            // A ChunkSelector is needed when no splitter is provided.
            chunkSelector = ChunkSelectorFactory.makeChunkSelector(context);
            if (chunkSelector == null) {
                throw new ConnectorException(String.format("To generate embeddings on documents, you must specify either " +
                        "%s or %s to define the location of chunks in documents.",
                    context.getOptionNameForMessage(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER),
                    context.getOptionNameForMessage(Options.WRITE_EMBEDDER_CHUNKS_XPATH)
                ));
            }
        }

        return textExtractor == null && textSplitter == null && textClassifier == null && embeddingProducer == null ?
            null :
            new DocumentPipeline(textExtractor, textSplitter, textClassifier, embeddingProducer, chunkSelector);
    }

    private static NuaClient newNuaClient(Context context) {
        // Check for mock option first (for testing)
        if (context.hasOption(MOCK_NUA_CLIENT_OPTION)) {
            String mockResponseJson = context.getStringOption(MOCK_NUA_CLIENT_OPTION);
            return new MockNuaClient(mockResponseJson);
        }

        String nuaKey = context.getProperties().get(Options.WRITE_NUCLIA_NUA_KEY);
        if (nuaKey == null || nuaKey.trim().isEmpty()) {
            return null;
        }

        DefaultNuaClient.Builder builder = DefaultNuaClient.builder(nuaKey);

        int timeout = context.getIntOption(Options.WRITE_NUCLIA_TIMEOUT, 120, 1);
        builder.withTimeout(timeout);

        String apiUrl = context.getProperties().get(Options.WRITE_NUCLIA_API_URL);
        if (apiUrl != null && !apiUrl.trim().isEmpty()) {
            builder.withApiUrl(apiUrl);
        }

        return builder.build();
    }

    private static TextSplitter newTextSplitter(Context context) {
        boolean shouldSplit = context.getProperties().keySet().stream().anyMatch(key -> key.startsWith(Options.WRITE_SPLITTER_PREFIX));
        if (!shouldSplit) {
            return null;
        }
        @SuppressWarnings("unchecked")
        TextSplitterFactory factory = (TextSplitterFactory) newLangchain4jProcessorFactory();
        return factory != null ? factory.newTextSplitter(context) : null;
    }

    private static EmbeddingProducer newEmbeddingProducer(Context context) {
        boolean shouldEmbed = context.getProperties().keySet().stream().anyMatch(key -> key.startsWith(Options.WRITE_EMBEDDER_PREFIX));
        if (!shouldEmbed) {
            return null;
        }
        @SuppressWarnings("unchecked")
        EmbeddingProducerFactory factory = (EmbeddingProducerFactory) newLangchain4jProcessorFactory();
        return factory != null ? factory.newEmbeddingProducer(context) : null;
    }

    private static Object newLangchain4jProcessorFactory() {
        return new Langchain4jFactory();
    }

    private DocumentPipelineFactory() {
    }
}
