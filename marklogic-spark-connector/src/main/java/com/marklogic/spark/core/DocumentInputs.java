/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
    private List<ChunkInputs> chunkInputsList;

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata) {
        this(initialUri, content, columnValuesForUriTemplate, initialMetadata, null);
    }

    public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                          DocumentMetadataHandle initialMetadata, String graph) {
        if (initialUri == null) {
            // This should only ever happen via a programming error.
            throw new IllegalArgumentException("initialUri cannot be null");
        }
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
        if (chunkInputsList == null || chunkInputsList.isEmpty()) {
            throw new IllegalStateException("Cannot add classification: no chunks exist");
        }
        // Find the next chunk without a classification
        for (ChunkInputs chunk : chunkInputsList) {
            if (chunk.getClassification() == null) {
                chunk.setClassification(classification);
                return;
            }
        }
        throw new IllegalStateException("Cannot add classification: all chunks already have classifications");
    }

    public void addEmbedding(float[] embedding, String modelName) {
        if (chunkInputsList == null || chunkInputsList.isEmpty()) {
            throw new IllegalStateException("Cannot add embedding: no chunks exist");
        }
        // Find the next chunk without an embedding
        for (ChunkInputs chunk : chunkInputsList) {
            if (chunk.getEmbedding() == null) {
                chunk.setEmbedding(embedding);
                chunk.setModelName(modelName);
                return;
            }
        }
        throw new IllegalStateException("Cannot add embedding: all chunks already have embeddings");
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
        if (chunkInputsList == null) {
            return null;
        }
        List<String> texts = new ArrayList<>(chunkInputsList.size());
        for (ChunkInputs chunk : chunkInputsList) {
            texts.add(chunk.getText());
        }
        return texts;
    }

    public void setChunks(List<String> chunks) {
        if (chunks == null) {
            this.chunkInputsList = null;
        } else {
            this.chunkInputsList = new ArrayList<>(chunks.size());
            for (String text : chunks) {
                this.chunkInputsList.add(new ChunkInputs(text));
            }
        }
    }

    /**
     * Adds a chunk with its embedding, model name, and metadata. This is useful for workflows like Nuclia where
     * chunks and embeddings are received together.
     *
     * @param text      the chunk text
     * @param embedding the embedding vector (can be null)
     * @param modelName the model name (can be null)
     * @param metadata  the metadata as a JsonNode (can be null)
     */
    public void addChunk(String text, float[] embedding, String modelName, JsonNode metadata) {
        if (chunkInputsList == null) {
            chunkInputsList = new ArrayList<>();
        }
        ChunkInputs chunkInputs = new ChunkInputs(text);
        if (embedding != null) {
            chunkInputs.setEmbedding(embedding);
            chunkInputs.setModelName(modelName);
        }
        chunkInputs.setMetadata(metadata);
        chunkInputsList.add(chunkInputs);
    }

    public byte[] getDocumentClassification() {
        return documentClassification;
    }

    public void setDocumentClassification(byte[] documentClassification) {
        this.documentClassification = documentClassification;
    }

    public List<ChunkInputs> getChunkInputsList() {
        return chunkInputsList;
    }
}
