/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.embedding.EmbeddingAdder;
import com.marklogic.langchain4j.embedding.EmbeddingAdderFactory;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.Context;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.embedding.EmbeddingProducerFactory;
import com.marklogic.spark.core.splitter.TextSplitter;
import com.marklogic.spark.core.splitter.TextSplitterFactory;
import dev.langchain4j.model.embedding.EmbeddingModel;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

public class Langchain4jDocumentProcessorFactory implements
    Function<Context, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>>,
    TextSplitterFactory,
    EmbeddingProducerFactory {

    public TextSplitter newTextSplitter(Context context) {
        Optional<DocumentTextSplitter> splitter = DocumentTextSplitterFactory.makeSplitter(context);
        return splitter.isPresent() ? splitter.get() : null;
    }

    @Override
    public EmbeddingProducer newEmbeddingProducer(Context context) {
        Optional<EmbeddingModel> embeddingModel = EmbeddingAdderFactory.makeEmbeddingModel(context);
        return embeddingModel.isPresent() ?
            EmbeddingAdderFactory.makeEmbeddingGenerator(context, embeddingModel.get()) :
            null;
    }

    public Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> apply(Context context) {
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
