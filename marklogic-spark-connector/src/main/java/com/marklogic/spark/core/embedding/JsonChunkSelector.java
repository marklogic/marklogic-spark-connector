/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.Util;

import java.util.ArrayList;
import java.util.List;

public class JsonChunkSelector implements ChunkSelector {

    private final JsonPointer chunksPointer;
    private final String textPointer;
    private final String embeddingArrayName;
    private final boolean base64EncodeVectors;

    public static class Builder {
        private String chunksPointer = "/chunks";
        private String textPointer;
        private String embeddingArrayName;
        private boolean base64EncodeVectors = false;

        public Builder withChunksPointer(String chunksPointer) {
            if (chunksPointer != null) {
                this.chunksPointer = chunksPointer;
            }
            return this;
        }

        public Builder withTextPointer(String textPointer) {
            if (textPointer != null) {
                this.textPointer = textPointer;
            }
            return this;
        }

        public Builder withEmbeddingArrayName(String embeddingArrayName) {
            if (embeddingArrayName != null) {
                this.embeddingArrayName = embeddingArrayName;
            }
            return this;
        }

        public Builder withBase64EncodeVectors(boolean base64EncodeVectors) {
            this.base64EncodeVectors = base64EncodeVectors;
            return this;
        }

        public JsonChunkSelector build() {
            return new JsonChunkSelector(chunksPointer, textPointer, embeddingArrayName, base64EncodeVectors);
        }
    }

    private JsonChunkSelector(String chunksPointerExpression, String textPointer, String embeddingArrayName, boolean base64EncodeVectors) {
        this.chunksPointer = JsonPointer.compile(chunksPointerExpression);
        this.textPointer = textPointer;
        this.embeddingArrayName = embeddingArrayName;
        this.base64EncodeVectors = base64EncodeVectors;
    }

    @Override
    public DocumentAndChunks selectChunks(String uri, AbstractWriteHandle content) {
        JsonNode doc = Util.getJsonFromHandle(content);
        JsonNode chunksNode = doc.at(chunksPointer);
        if (chunksNode == null || (!(chunksNode instanceof ArrayNode) && !(chunksNode instanceof ObjectNode))) {
            return null;
        }

        List<Chunk> chunks = new ArrayList<>();
        if (chunksNode instanceof ArrayNode) {
            chunksNode.forEach(obj -> {
                JsonChunk chunk = new JsonChunk((ObjectNode) obj, textPointer, embeddingArrayName, base64EncodeVectors);
                if (chunk.hasEmbeddingText()) {
                    chunks.add(chunk);
                }
            });
        } else {
            JsonChunk chunk = new JsonChunk((ObjectNode) chunksNode, textPointer, embeddingArrayName, base64EncodeVectors);
            if (chunk.hasEmbeddingText()) {
                chunks.add(chunk);
            }
        }
        DocumentWriteOperation documentToWrite = new DocumentWriteOperationImpl(uri, null, new JacksonHandle(doc));
        return new DocumentAndChunks(documentToWrite, chunks);
    }
}
