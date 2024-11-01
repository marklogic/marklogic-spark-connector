/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.writer.JsonUtil;

import java.util.ArrayList;
import java.util.List;

class JsonChunkSelector implements ChunkSelector {

    private final JsonPointer chunksPointer;
    private final JsonPointer textPointer;
    private final String embeddingArrayName;

    static class Builder {
        String chunksPointer = "/chunks";
        String textPointer = "/text";
        String embeddingArrayName = "embedding";

        Builder withChunksPointer(String chunksPointer) {
            if (chunksPointer != null) {
                this.chunksPointer = chunksPointer;
            }
            return this;
        }

        Builder withTextPointer(String textPointer) {
            if (textPointer != null) {
                this.textPointer = textPointer;
            }
            return this;
        }

        Builder withEmbeddingArrayName(String embeddingArrayName) {
            if (embeddingArrayName != null) {
                this.embeddingArrayName = embeddingArrayName;
            }
            return this;
        }

        JsonChunkSelector build() {
            return new JsonChunkSelector(chunksPointer, textPointer, embeddingArrayName);
        }
    }

    private JsonChunkSelector(String chunksPointerExpression, String textPointerExpression, String embeddingArrayName) {
        this.chunksPointer = JsonPointer.compile(chunksPointerExpression);
        this.textPointer = JsonPointer.compile(textPointerExpression);
        this.embeddingArrayName = embeddingArrayName;
    }

    @Override
    public DocumentAndChunks selectChunks(DocumentWriteOperation sourceDocument) {
        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDocument.getContent());

        JsonNode chunksNode = doc.at(chunksPointer);
        List<Chunk> chunks = new ArrayList<>();
        if (chunksNode == null) {
            return new DocumentAndChunks(sourceDocument, null);
        }

        if (chunksNode instanceof ArrayNode) {
            chunksNode.forEach(obj -> chunks.add(new JsonChunk((ObjectNode) obj, textPointer, embeddingArrayName)));
        } else if (chunksNode instanceof ObjectNode) {
            chunks.add(new JsonChunk((ObjectNode) chunksNode, textPointer, embeddingArrayName));
        } else {
            // No valid chunks found, just return the original document.
            return new DocumentAndChunks(sourceDocument, null);
        }

        DocumentWriteOperation documentToWrite = new DocumentWriteOperationImpl(
            sourceDocument.getUri(), sourceDocument.getMetadata(), new JacksonHandle(doc)
        );
        return new DocumentAndChunks(documentToWrite, chunks);
    }
}
