/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.embedding.Embedding;

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

    @Override
    public String getDocumentUri() {
        return documentUri;
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
