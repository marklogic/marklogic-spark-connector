/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.ContentDescriptor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractWriteHandle;

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
        this.content = content;
        this.columnValuesForUriTemplate = columnValuesForUriTemplate;
        this.initialMetadata = initialMetadata;
        this.graph = graph;
    }

    public void overrideContent(AbstractWriteHandle modifiedContent) {
        // For when chunks have been selected.
        this.content = modifiedContent;
    }

    /**
     * For the 1.3 release, we don't yet support "single article" mode where a binary can be submitted to Semaphore.
     * Everything is "multi article" which requires storing text from a document in an XML document that is sent to
     * Semaphore. This method helps avoid errors where a raw binary could be inserted into the XML document and cause
     * parse errors.
     *
     * @return
     */
    public boolean hasClassifiableText() {
        if (extractedText != null && !extractedText.trim().isEmpty()) {
            return true;
        }

        if (content instanceof ContentDescriptor) {
            ContentDescriptor contentDescriptor = (ContentDescriptor) this.content;
            Format format = contentDescriptor.getFormat();
            return Format.XML.equals(format) || Format.JSON.equals(format) || Format.TEXT.equals(format);
        }

        return false;
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
