/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.JsonUtil;

import java.util.Iterator;

public class JsonChunkSelector implements ChunkSelector {

    private final JsonPointer chunksPointer;

    public JsonChunkSelector() {
        this("/chunks");
    }

    public JsonChunkSelector(String jsonPointerExpression) {
        this.chunksPointer = JsonPointer.compile(jsonPointerExpression);
    }

    @Override
    public Iterator<Chunk> selectChunks(DocumentWriteOperation sourceDocument) {
        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDocument.getContent());
        return new JsonChunksIterator(doc.at(chunksPointer).iterator());
    }

    private static class JsonChunksIterator implements Iterator<Chunk> {

        private Iterator<JsonNode> chunks;

        JsonChunksIterator(Iterator<JsonNode> chunks) {
            this.chunks = chunks;
        }

        @Override
        public boolean hasNext() {
            return this.chunks.hasNext();
        }

        @Override
        public Chunk next() {
            return new JsonChunk((ObjectNode) this.chunks.next());
        }
    }
}
