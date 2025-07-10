/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.util.VectorUtil;

public class JsonChunk implements Chunk {

    private final String documentUri;
    private final ObjectNode chunk;
    private final JsonPointer textPointer;
    private final String embeddingArrayName;
    private final boolean base64EncodeVectors;

    public JsonChunk(String documentUri, ObjectNode chunk, String textPointer, String embeddingArrayName, boolean base64EncodeVectors) {
        this.documentUri = documentUri;
        this.chunk = chunk;
        this.textPointer = JsonPointer.compile(textPointer != null ? textPointer : "/text");
        this.embeddingArrayName = embeddingArrayName != null ? embeddingArrayName : "embedding";
        this.base64EncodeVectors = base64EncodeVectors;
    }

    public boolean hasEmbeddingText() {
        String text = getEmbeddingText();
        return text != null && !text.trim().isEmpty();
    }

    @Override
    public String getDocumentUri() {
        return documentUri;
    }

    @Override
    public String getEmbeddingText() {
        return chunk.at(this.textPointer).asText();
    }

    @Override
    public void addEmbedding(float[] embedding) {
        if (base64EncodeVectors) {
            String base64Vector = VectorUtil.base64Encode(embedding);
            chunk.put(this.embeddingArrayName, base64Vector);
            // Add language as a top-level property to disable stemming in MarkLogic
            chunk.put("language", "zxx");
        } else {
            addEmbeddingAsArray(embedding);
        }
    }

    private void addEmbeddingAsArray(float[] embedding) {
        ArrayNode array = chunk.putArray(this.embeddingArrayName);
        for (int i = 0; i < embedding.length; i++) {
            array.add(embedding[i]);
        }
    }
}
