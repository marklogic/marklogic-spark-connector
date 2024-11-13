/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.writer.embedding.EmbedderDocumentProcessorFactory;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessor;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessorFactory;

import java.util.Optional;

abstract class DocumentProcessorFactory {

    static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        Optional<SplitterDocumentProcessor> splitter = SplitterDocumentProcessorFactory.makeSplitter(context);

        Optional<DocumentProcessor> embedder = EmbedderDocumentProcessorFactory.makeEmbedder(
            context, splitter.isPresent() ? splitter.get() : null
        );

        if (embedder.isPresent()) {
            return embedder.get();
        }
        return splitter.isPresent() ? splitter.get() : null;
    }

    private DocumentProcessorFactory() {
    }
}
