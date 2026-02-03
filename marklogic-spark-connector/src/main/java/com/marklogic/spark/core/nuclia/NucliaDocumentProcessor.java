/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.nuclia;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Processes documents through Nuclia's API for text extraction, chunking, and embedding generation.
 *
 * @since 3.1.0
 */
public class NucliaDocumentProcessor {

    private final NucliaClient nucliaClient;

    public NucliaDocumentProcessor(NucliaClient nucliaClient) {
        this.nucliaClient = nucliaClient;
    }

    /**
     * Processes a list of document inputs through Nuclia.
     * Each document's binary content is uploaded to Nuclia, which returns extracted text, chunks, and embeddings.
     *
     * @param inputs the list of document inputs to process
     */
    public void processDocuments(List<DocumentInputs> inputs) {
        for (DocumentInputs input : inputs) {
            try {
                // Get binary content for file upload to Nuclia
                byte[] content = input.getContentAsBytes();
                if (content == null || content.length == 0) {
                    continue;
                }

                // Extract filename from URI (e.g., "/path/to/file.pdf" -> "file.pdf")
                String filename = extractFilename(input.getInitialUri());

                // Process through Nuclia using binary file upload workflow
                // Collect to list since we need all the data anyway
                List<ObjectNode> results = nucliaClient.processData(filename, content)
                    .collect(Collectors.toList());

                if (results.isEmpty()) {
                    continue;
                }

                // First ObjectNode is the extracted text
                ObjectNode firstNode = results.get(0);
                String extractedText = extractTextFromNucliaNode(firstNode);
                if (extractedText != null && !extractedText.isEmpty()) {
                    input.setExtractedText(extractedText);
                }

                // Rest are chunks with embeddings - extract all data for each chunk at once
                for (int i = 1; i < results.size(); i++) {
                    ObjectNode chunkNode = results.get(i);
                    addChunkFromNucliaNode(chunkNode, input);
                }

            } catch (IOException | InterruptedException e) {
                Util.MAIN_LOGGER.warn("Failed to process document with Nuclia: {}, error: {}", input.getInitialUri(), e.getMessage());
                // Continue processing other documents - this one will be written without Nuclia data
            }
        }
    }

    /**
     * Extracts the full text from the first Nuclia SSE event node.
     * The node structure is:
     * {
     *   "type": "FullText",
     *   "field": "content",
     *   "field_type": "TEXT",
     *   "text": "extracted text content..."
     * }
     */
    private String extractTextFromNucliaNode(ObjectNode node) {
        if (node.has("text")) {
            return node.get("text").asText();
        }
        return null;
    }

    /**
     * Extracts chunk data from a Nuclia SSE chunk event node and adds it to the document input.
     * The node structure is:
     * {
     *   "type": "Chunk",
     *   "text": "chunk text content...",
     *   "embeddings": [
     *     {
     *       "id": "multilingual-2024-05-06",
     *       "embedding": [0.123, -0.456, ...]
     *     }
     *   ]
     * }
     * If multiple embeddings are present, creates a separate chunk for each embedding with the same text.
     */
    private void addChunkFromNucliaNode(ObjectNode node, DocumentInputs input) {
        // Extract text
        String text = node.has("text") ? node.get("text").asText() : null;
        if (text == null || text.isEmpty()) {
            return;
        }

        // Process each embedding in the array
        if (node.has("embeddings") && node.get("embeddings").isArray()) {
            var embeddingsArray = node.get("embeddings");

            for (int i = 0; i < embeddingsArray.size(); i++) {
                var embeddingObj = embeddingsArray.get(i);
                float[] embedding = null;
                String modelName = null;

                if (embeddingObj.has("embedding") && embeddingObj.get("embedding").isArray()) {
                    var embeddingArray = embeddingObj.get("embedding");
                    int size = embeddingArray.size();
                    embedding = new float[size];
                    for (int j = 0; j < size; j++) {
                        embedding[j] = (float) embeddingArray.get(j).asDouble();
                    }
                }

                if (embeddingObj.has("id")) {
                    modelName = embeddingObj.get("id").asText();
                }

                input.addChunk(text, embedding, modelName);
            }
        } else {
            // No embeddings, still add the chunk with just text
            input.addChunk(text, null, null);
        }
    }

    /**
     * Extracts the filename from a URI path.
     * For example: "/path/to/file.pdf" returns "file.pdf"
     */
    private String extractFilename(String uri) {
        if (uri == null || uri.isEmpty()) {
            return "document";
        }
        int lastSlash = uri.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < uri.length() - 1) {
            return uri.substring(lastSlash + 1);
        }
        return uri;
    }
}
