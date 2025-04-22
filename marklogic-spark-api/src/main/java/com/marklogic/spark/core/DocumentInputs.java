/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.BufferableHandle;
import com.marklogic.spark.ConnectorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private Map<String, String> extractedMetadata;

    private byte[] documentClassification;
    private List<byte[]> chunkClassifications;
    private List<float[]> embeddings;

    // These will be created via a splitter.
    private List<String> chunks;

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata) {
        this(initialUri, content, columnValuesForUriTemplate, initialMetadata, null);
    }

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata, String graph) {
        this.initialUri = initialUri;
        setContent(content);
        this.columnValuesForUriTemplate = columnValuesForUriTemplate;
        this.initialMetadata = initialMetadata;
        this.graph = graph;
    }

    private void setContent(AbstractWriteHandle content) {
        if (content != null && !(content instanceof BufferableHandle)) {
            // This should only ever happen via a programming error. The intent is to ensure that getContentAsBytes
            // will work for any content handle. Ideally, the Java Client would just have something like a
            // BufferableWriteHandle to avoid this kind of stuff.
            throw new ConnectorException("System error; content must be a BufferableHandle.");
        }
        this.content = content;
    }

    public void overrideContent(AbstractWriteHandle modifiedContent) {
        // For when chunks have been selected.
        setContent(modifiedContent);
    }

    public byte[] getContentAsBytes() {
        return content != null ? ((BufferableHandle) content).toBuffer() : null;
    }

    public AbstractWriteHandle getContent() {
        return content;
    }

    public void addChunkClassification(byte[] classification) {
        if (chunkClassifications == null) {
            chunkClassifications = new ArrayList<>();
        }
        chunkClassifications.add(classification);
    }

    public void addEmbedding(float[] embedding) {
        if (embeddings == null) {
            embeddings = new ArrayList<>();
        }
        embeddings.add(embedding);
    }

    public String getInitialUri() {
        return initialUri;
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

    public Map<String, String> getExtractedMetadata() {
        return extractedMetadata;
    }

    public void setExtractedMetadata(Map<String, String> extractedMetadata) {
        this.extractedMetadata = extractedMetadata;
    }

    public List<String> getChunks() {
        return chunks;
    }

    public void setChunks(List<String> chunks) {
        this.chunks = chunks;
    }

    public List<byte[]> getClassifications() {
        return chunkClassifications;
    }

    public byte[] getDocumentClassification() {
        return documentClassification;
    }

    public void setDocumentClassification(byte[] documentClassification) {
        this.documentClassification = documentClassification;
    }

    public List<float[]> getEmbeddings() {
        return embeddings;
    }
}
