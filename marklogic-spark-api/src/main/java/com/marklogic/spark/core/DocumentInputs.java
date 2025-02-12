/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.core.embedding.Chunk;

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

    public String getInitialUri() {
        return initialUri;
    }

    public void setContent(AbstractWriteHandle content) {
        this.content = content;
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

    public void setExistingChunks(List<Chunk> existingChunks) {
        this.existingChunks = existingChunks;
    }
}
