/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DocumentAndChunks;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Captures the various inputs used for constructing a document to be written to MarkLogic. {@code graph} refers
 * to an optional MarkLogic semantics graph, which must be added to the final set of collections for the
 * document.
 */
public class DocumentInputs {

    private final String initialUri;
    private AbstractWriteHandle content;
    private final JsonNode columnValuesForUriTemplate;
    private final DocumentMetadataHandle initialMetadata;
    private final String graph;

    private String extractedText;
    private List<byte[]> classifications;
    private byte[] classificationResponse;
    private List<float[]> embeddings;

    // These will be created via a splitter.
    private List<String> chunks;

    // While these are for when the user wants to create embeddings on a document that already has chunks.
    private List<Chunk> existingChunks;

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata) {
        this(initialUri, content, columnValuesForUriTemplate, initialMetadata, null);
    }

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata, String graph) {
        this.initialUri = initialUri;
        this.content = content;
        this.columnValuesForUriTemplate = columnValuesForUriTemplate;
        this.initialMetadata = initialMetadata;
        this.graph = graph;
    }

    public List<String> getTextSegmentsToGenerateEmbeddings() {
        // Shields the embedder from having to know whether to look for just-split chunks or existing chunks.
        if (existingChunks != null && !existingChunks.isEmpty()) {
            return this.existingChunks.stream().map(Chunk::getEmbeddingText).collect(Collectors.toList());
        }
        return this.chunks;
    }

    public void setGeneratedEmbeddings(List<float[]> generatedEmbeddings) {
        // Requires the embedder to pass the entire set of embeddings for this document in one call. This avoids
        // tricky situations with existing chunks, where we don't have a simple "add to the next existing chunk" method.
        if (existingChunks != null && !existingChunks.isEmpty()) {
            for (int i = 0; i < generatedEmbeddings.size(); i++) {
                existingChunks.get(i).addEmbedding(generatedEmbeddings.get(i));
            }
        } else {
            if (embeddings == null) {
                embeddings = new ArrayList<>();
            }
            embeddings.addAll(generatedEmbeddings);
        }
    }

    /**
     * Overrides the content handle, as in the process of selecting existing chunks, a new content object may have been
     * created that the existing chunks are a part of. For example, chunks represented by DOM {@code Element}
     * instances will be part of a parent DOM {@code Document} object that was created during the chunk selection
     * processing. That {@code Document} object needs to be the content associated with this inputs object.
     *
     * @param documentAndChunks
     */
    public void setContentAndExistingChunks(DocumentAndChunks documentAndChunks) {
        this.content = documentAndChunks.getContent();
        this.existingChunks = documentAndChunks.getChunks();
    }

    public String getInitialUri() {
        return initialUri;
    }

    public AbstractWriteHandle getContent() {
        return content;
    }

    public JsonNode getColumnValuesForUriTemplate() {
        return columnValuesForUriTemplate;
    }

    public DocumentMetadataHandle getInitialMetadata() {
        return initialMetadata;
    }

    public String getGraph() {
        return graph;
    }

    public String getExtractedText() {
        return extractedText;
    }

    public void setExtractedText(String extractedText) {
        this.extractedText = extractedText;
    }

    public List<String> getChunks() {
        return chunks;
    }

    public void setChunks(List<String> chunks) {
        this.chunks = chunks;
    }

    public List<byte[]> getClassifications() {
        return classifications;
    }

    public void setClassifications(List<byte[]> classifications) {
        this.classifications = classifications;
    }

    public byte[] getClassificationResponse() {
        return classificationResponse;
    }

    public void setClassificationResponse(byte[] classificationResponse) {
        this.classificationResponse = classificationResponse;
    }

    public List<float[]> getEmbeddings() {
        return embeddings;
    }
}
