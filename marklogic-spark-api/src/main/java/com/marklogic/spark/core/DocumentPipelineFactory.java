/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.classifier.TextClassifierFactory;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.ChunkSelectorFactory;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.embedding.EmbeddingProducerFactory;
import com.marklogic.spark.core.extraction.TextExtractor;
import com.marklogic.spark.core.extraction.TikaTextExtractor;
import com.marklogic.spark.core.splitter.TextSplitter;
import com.marklogic.spark.core.splitter.TextSplitterFactory;

import java.lang.reflect.InvocationTargetException;

public abstract class DocumentPipelineFactory {

    private static final String FACTORY_CLASS_NAME = "com.marklogic.langchain4j.Langchain4jFactory";

    public static DocumentPipeline newDocumentPipeline(Context context) {
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

        return textExtractor != null || textSplitter != null || textClassifier != null || embeddingProducer != null ?
            new DocumentPipeline(textExtractor, textSplitter, textClassifier, embeddingProducer, chunkSelector) :
            null;
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
        try {
            return Class.forName(FACTORY_CLASS_NAME).getDeclaredConstructor().newInstance();
        } catch (UnsupportedClassVersionError e) {
            throw new ConnectorException("Unable to configure support for splitting documents and/or generating embeddings. " +
                "Please ensure you are using Java 17 or higher for these operations.", e);
        }
        // Catch every checked exception from trying to instantiate the class. Any exception from the factory class
        // itself is expected to be a RuntimeException that should bubble up.
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
               InvocationTargetException ex) {
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug(
                    "Unable to instantiate factory class {}; this is expected when the marklogic-langchain4j module is not on the classpath. Cause: {}",
                    FACTORY_CLASS_NAME, ex.getMessage()
                );
            }
            return null;
        }
    }

    private DocumentPipelineFactory() {
    }
}
