/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.Util;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Supports a use case where a document already has chunks in it, which must be selected via a {@code ChunkSelector}.
 * The {@code EmbeddingModel} is then used to generate and add an embedding to each chunk in a given document.
 */
public class EmbeddingAdder implements Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>, Supplier<Iterator<DocumentWriteOperation>> {

    private final ChunkSelector chunkSelector;
    private final EmbeddingGenerator embeddingGenerator;
    private final DocumentTextSplitter documentTextSplitter;

    private List<DocumentWriteOperation> pendingSourceDocuments = new ArrayList<>();

    /**
     * Use this when a user has configured a splitter, as the splitter will return {@code DocumentAndChunks} instances
     * that avoid the need for using a {@code ChunkSelector} to find chunks.
     *
     * @param documentTextSplitter
     * @param embeddingGenerator
     */
    public EmbeddingAdder(DocumentTextSplitter documentTextSplitter, EmbeddingGenerator embeddingGenerator) {
        this.documentTextSplitter = documentTextSplitter;
        this.embeddingGenerator = embeddingGenerator;
        this.chunkSelector = null;
    }

    /**
     * Use this constructor when the user has not configured a splitter, as the {@code ChunkSelector} is needed to find
     * chunks in each document.
     *
     * @param chunkSelector
     * @param embeddingGenerator
     */
    public EmbeddingAdder(ChunkSelector chunkSelector, EmbeddingGenerator embeddingGenerator) {
        this.chunkSelector = chunkSelector;
        this.embeddingGenerator = embeddingGenerator;
        this.documentTextSplitter = null;
    }

    @Override
    public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
        // If the user configured a splitter, then follow a path where the source document is split, which will produce
        // DocumentAndChunks instances. Which means the ChunkSelector isn't needed.
        if (documentTextSplitter != null) {
            return splitAndAddEmbeddings(sourceDocument);
        }

        DocumentAndChunks documentAndChunks = chunkSelector.selectChunks(sourceDocument);
        return documentAndChunks.hasChunks() ?
            addEmbeddingsToExistingChunks(documentAndChunks) :
            // If no chunks are found, embeddings can't be added, so just return the source document.
            Stream.of(documentAndChunks.getDocumentToWrite()).iterator();
    }

    @Override
    public Iterator<DocumentWriteOperation> get() {
        // Return any pending source documents - i.e. those with chunks that didn't add up to the embedding generator's
        // batch size, and thus embeddings haven't been added.
        if (pendingSourceDocuments != null && !pendingSourceDocuments.isEmpty()) {
            if (Util.LANGCHAIN4J_LOGGER.isInfoEnabled()) {
                Util.LANGCHAIN4J_LOGGER.info("Pending source document count: {}; generating embeddings for each document.",
                    pendingSourceDocuments.size());
            }
            embeddingGenerator.generateEmbeddingsForPendingChunks();
            return pendingSourceDocuments.iterator();
        }
        return Stream.<DocumentWriteOperation>empty().iterator();
    }

    private Iterator<DocumentWriteOperation> splitAndAddEmbeddings(DocumentWriteOperation sourceDocument) {
        Iterator<DocumentWriteOperation> splitDocuments = documentTextSplitter.apply(sourceDocument);

        // Track the list of documents to return. A document won't be returned immediately if it has chunks but the
        // embedding generator doesn't receive enough chunks to meet its batch size threshold.
        List<DocumentWriteOperation> documentsToReturn = new ArrayList<>();

        splitDocuments.forEachRemaining(splitDoc -> {
            boolean hasChunks = splitDoc instanceof DocumentAndChunks && ((DocumentAndChunks) splitDoc).hasChunks();
            if (hasChunks) {
                DocumentAndChunks documentAndChunks = (DocumentAndChunks) splitDoc;
                pendingSourceDocuments.add(documentAndChunks);
                boolean embeddingsWereGenerated = embeddingGenerator.addEmbeddings(documentAndChunks);
                // If the embedding generator received enough chunks to exceed its batch size, then all the pending
                // documents can be added to the list of documents to return, as we know those documents will have had
                // embeddings added to them.
                if (embeddingsWereGenerated) {
                    documentsToReturn.addAll(pendingSourceDocuments);
                    pendingSourceDocuments.clear();
                }
            } else {
                // If the document doesn't have any chunks, it can be returned immediately.
                documentsToReturn.add(splitDoc);
            }
        });

        return documentsToReturn.iterator();
    }

    /**
     * For existing chunks - add the document to the pending list. Then add embeddings. If embeddings were generated,
     * return an iterator over all the pending documents, which now have embeddings.
     */
    private Iterator<DocumentWriteOperation> addEmbeddingsToExistingChunks(DocumentAndChunks documentAndChunks) {
        pendingSourceDocuments.add(documentAndChunks);
        boolean embeddingsWereGenerated = embeddingGenerator.addEmbeddings(documentAndChunks);
        if (embeddingsWereGenerated) {
            List<DocumentWriteOperation> documentsWithEmbeddings = new ArrayList<>();
            documentsWithEmbeddings.addAll(pendingSourceDocuments);
            pendingSourceDocuments.clear();
            return documentsWithEmbeddings.iterator();
        } else {
            return Stream.<DocumentWriteOperation>empty().iterator();
        }
    }
}
