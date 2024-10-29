/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.embedding.Embedding;

/**
 * Assumes the default structure produced by the connector's splitter feature.
 */
public class JsonChunk implements Chunk {

    private ObjectNode chunk;

    public JsonChunk(ObjectNode chunk) {
        this.chunk = chunk;
    }

    @Override
    public String getEmbeddingText() {
        return chunk.get("text").asText();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        ArrayNode array = chunk.putArray("embedding");
        for (float val : embedding.vector()) {
            array.add(Float.toString(val));
        }
    }
}
