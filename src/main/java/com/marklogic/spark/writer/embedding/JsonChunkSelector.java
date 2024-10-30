/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.writer.JsonUtil;

import java.util.ArrayList;
import java.util.List;

class JsonChunkSelector implements ChunkSelector {

    private final JsonPointer chunksPointer;

    JsonChunkSelector() {
        this("/chunks");
    }

    JsonChunkSelector(String jsonPointerExpression) {
        this.chunksPointer = JsonPointer.compile(jsonPointerExpression);
    }

    @Override
    public DocumentAndChunks selectChunks(DocumentWriteOperation sourceDocument) {
        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDocument.getContent());

        JsonNode chunksNode = doc.at(chunksPointer);
        List<Chunk> chunks = new ArrayList<>();
        if (chunksNode == null) {
            return new DocumentAndChunks(sourceDocument, null);
        }

        chunksNode.forEach(obj -> chunks.add(new JsonChunk((ObjectNode) obj)));

        DocumentWriteOperation documentToWrite = new DocumentWriteOperationImpl(
            sourceDocument.getUri(), sourceDocument.getMetadata(), new JacksonHandle(doc)
        );
        
        return new DocumentAndChunks(documentToWrite, chunks);
    }
}
