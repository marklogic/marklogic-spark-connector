/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.DocumentProcessor;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.Iterator;
import java.util.stream.Stream;

public class EmbeddingDocumentProcessor implements DocumentProcessor {

    private final ChunkSelector chunkSelector;
    private final EmbeddingModel embeddingModel;

    public EmbeddingDocumentProcessor(EmbeddingModel embeddingModel) {
        this.chunkSelector = new JsonChunkSelector();
        this.embeddingModel = embeddingModel;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        chunkSelector.selectChunks(sourceDocument).forEachRemaining(chunk -> {
            Response<Embedding> response = embeddingModel.embed(chunk.getEmbeddingText());
            chunk.addEmbedding(response.content());
        });
        return Stream.of(sourceDocument).iterator();
    }
}
