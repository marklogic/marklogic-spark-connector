/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.embedding.Embedding;

public class JsonChunk implements Chunk {

    private final ObjectNode chunk;
    private final JsonPointer textPointer;
    private final String embeddingArrayName;

    public JsonChunk(ObjectNode chunk) {
        this(chunk, JsonPointer.compile("/text"), "embedding");
    }

    public JsonChunk(ObjectNode chunk, JsonPointer textPointer, String embeddingArrayName) {
        this.chunk = chunk;
        this.textPointer = textPointer;
        this.embeddingArrayName = embeddingArrayName;
    }

    @Override
    public String getEmbeddingText() {
        return chunk.at(this.textPointer).asText();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        ArrayNode array = chunk.putArray(this.embeddingArrayName);
        for (float val : embedding.vector()) {
            array.add(Float.toString(val));
        }
    }
}
