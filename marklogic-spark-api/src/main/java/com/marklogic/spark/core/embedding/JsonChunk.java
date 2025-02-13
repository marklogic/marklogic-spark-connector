/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonChunk implements Chunk {

    private final String documentUri;
    private final ObjectNode chunk;
    private final JsonPointer textPointer;
    private final String embeddingArrayName;

    public JsonChunk(String documentUri, ObjectNode chunk) {
        this(documentUri, chunk, null, null);
    }

    public JsonChunk(String documentUri, ObjectNode chunk, String textPointer, String embeddingArrayName) {
        this.documentUri = documentUri;
        this.chunk = chunk;
        this.textPointer = JsonPointer.compile(textPointer != null ? textPointer : "/text");
        this.embeddingArrayName = embeddingArrayName != null ? embeddingArrayName : "embedding";
    }

    public boolean hasEmbeddingText() {
        String text = getEmbeddingText();
        return text != null && text.trim().length() > 0;
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
        ArrayNode array = chunk.putArray(this.embeddingArrayName);
        for (int i = 0; i < embedding.length; i++) {
            array.add(embedding[i]);
        }
    }
}
