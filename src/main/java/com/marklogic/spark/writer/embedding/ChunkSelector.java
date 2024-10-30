/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.List;

/**
 * Abstracts how chunks are selected from a JSON or XML document.
 */
public interface ChunkSelector {

    /**
     * @param sourceDocument
     * @return Selecting chunks may involve deserializing a string or byte array into an e.g. JsonNode, in which case
     * the document to be written will not be the sourceDocument that is passed in.
     */
    DocumentAndChunks selectChunks(DocumentWriteOperation sourceDocument);

    class DocumentAndChunks {

        private final DocumentWriteOperation documentToWrite;
        private final List<Chunk> chunks;

        DocumentAndChunks(DocumentWriteOperation documentToWrite, List<Chunk> chunks) {
            this.documentToWrite = documentToWrite;
            this.chunks = chunks;
        }

        public DocumentWriteOperation getDocumentToWrite() {
            return documentToWrite;
        }

        public List<Chunk> getChunks() {
            return chunks;
        }
    }
}
