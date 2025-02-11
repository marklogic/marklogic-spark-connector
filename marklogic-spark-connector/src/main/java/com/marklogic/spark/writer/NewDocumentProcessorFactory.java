/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.classifier.TextClassifierFactory;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.ChunkSelectorFactory;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.tika.Tika;

public interface NewDocumentProcessorFactory {

    static NewDocumentProcessor newDocumentProcessor(Context context) {
        final Tika tika = context.getBooleanOption(Options.WRITE_EXTRACTED_TEXT, false) ?
            new Tika() : null;
        final TextSplitter textSplitter = DocumentProcessorFactory.newTextSplitter(context);
        final TextClassifier textClassifier = TextClassifierFactory.newTextClassifier(context);
        final EmbeddingProducer embeddingProducer = DocumentProcessorFactory.newEmbeddingProducer(context);

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
        return new NewDocumentProcessor(tika, textSplitter, textClassifier, embeddingProducer, chunkSelector);
    }

}
