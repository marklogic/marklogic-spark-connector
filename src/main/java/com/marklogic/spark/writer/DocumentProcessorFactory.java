/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.embedding.EmbeddingAdder;
import com.marklogic.spark.langchain4j.EmbeddingAdderFactory;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.langchain4j.DocumentTextSplitterFactory;
import com.marklogic.spark.ContextSupport;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * So we want the marklogic.spark code to refer to the OPtions class and the Spark-specific options.
 * It seems like we need builders here then.
 * <p>
 * OR we really have 3 modules - the connector; the langchain4j module; and then a 3rd module that bridges the two.
 */
abstract class DocumentProcessorFactory {

    static Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> buildDocumentProcessor(ContextSupport context) {
        // Once we shift to langchain 0.36 or higher and thus need Java 17, this will instead check the classpath
        // for an optional class for supporting splitting/embedding so that we don't have a tight coupling to
        // langchain4j.
        Optional<DocumentTextSplitter> splitter = DocumentTextSplitterFactory.makeSplitter(context);

        Optional<EmbeddingAdder> embedder = EmbeddingAdderFactory.makeEmbedder(
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
