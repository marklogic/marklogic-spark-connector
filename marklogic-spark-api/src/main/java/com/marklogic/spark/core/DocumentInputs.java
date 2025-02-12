/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.List;

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

    // Using hacky getter/setter for now until this is proven to work well.
    private String extractedText;
    private List<String> chunks;
    private List<byte[]> classifications;
    private byte[] classificationResponse;
    private List<float[]> embeddings;

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

    public void setEmbeddings(List<float[]> embeddings) {
        this.embeddings = embeddings;
    }
}
