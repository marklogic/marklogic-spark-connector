/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;

import java.util.List;

/**
 * Encapsulates a document to be written to MarkLogic along with an optional list of chunks that have been extracted
 * from it. Capturing the list of chunks is useful when a user wishes to use both the splitter and embedder. In that
 * scenario, the embedder can reuse the list of chunks produced by the splitter without having to find the chunks
 * itself.
 */
public class DocumentAndChunks implements DocumentWriteOperation {

    private final DocumentWriteOperation documentToWrite;
    private final List<Chunk> chunks;

    public DocumentAndChunks(DocumentWriteOperation documentToWrite, List<Chunk> chunks) {
        this.documentToWrite = documentToWrite;
        this.chunks = chunks;
    }

    public DocumentWriteOperation getDocumentToWrite() {
        return documentToWrite;
    }

    public List<Chunk> getChunks() {
        return chunks;
    }

    public boolean hasChunks() {
        return chunks != null && !chunks.isEmpty();
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.DOCUMENT_WRITE;
    }

    @Override
    public String getUri() {
        return documentToWrite.getUri();
    }

    @Override
    public DocumentMetadataWriteHandle getMetadata() {
        return documentToWrite.getMetadata();
    }

    @Override
    public AbstractWriteHandle getContent() {
        return documentToWrite.getContent();
    }

    @Override
    public String getTemporalDocumentURI() {
        return documentToWrite.getTemporalDocumentURI();
    }

    @Override
    public int compareTo(DocumentWriteOperation o) {
        return documentToWrite.compareTo(o);
    }
}
