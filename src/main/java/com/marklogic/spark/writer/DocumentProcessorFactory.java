/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.writer.embedding.EmbedderDocumentProcessorFactory;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessorFactory;

import java.util.Optional;

abstract class DocumentProcessorFactory {

    static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        Optional<DocumentProcessor> splitter = SplitterDocumentProcessorFactory.makeSplitter(context);
        if (splitter.isPresent()) {
            return splitter.get();
        }
        Optional<DocumentProcessor> embedder = EmbedderDocumentProcessorFactory.makeEmbedder(context);
        return embedder.isPresent() ? embedder.get() : null;
    }

    private DocumentProcessorFactory() {
    }
}
