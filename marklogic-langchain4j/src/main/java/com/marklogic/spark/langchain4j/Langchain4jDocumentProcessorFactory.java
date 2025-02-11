/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.embedding.EmbeddingAdder;
import com.marklogic.langchain4j.embedding.EmbeddingAdderFactory;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.Context;
import com.marklogic.spark.core.splitter.TextSplitter;
import com.marklogic.spark.core.splitter.TextSplitterFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

public class Langchain4jDocumentProcessorFactory implements Function<Context, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>>, TextSplitterFactory {

    public TextSplitter newTextSplitter(Context context) {
        Optional<DocumentTextSplitter> splitter = DocumentTextSplitterFactory.makeSplitter(context);

        Optional<EmbeddingAdder> embedder = EmbeddingAdderFactory.makeEmbedder(
            context, splitter.isPresent() ? splitter.get() : null
        );
        // Temporarily keeping the embedder tests happy until they can use this new approach.
        if (embedder.isPresent()) {
            return null;
        }

        return splitter.isPresent() ? splitter.get() : null;
    }

    public Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> apply(Context context) {
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

        // Turn off the splitter on this path, try to force NewDocumentProcessor to be used.
        // The above path will still be used by tests that do both embedding and splitting.
        return null;
    }

}
