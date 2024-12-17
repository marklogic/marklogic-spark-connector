/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.Format;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Defines common logic for creating JSON and XML chunk documents.
 */
abstract class AbstractChunkDocumentProducer implements Iterator<DocumentWriteOperation> {

    protected final DocumentWriteOperation sourceDocument;
    protected final List<TextSegment> textSegments;
    protected final ChunkConfig chunkConfig;
    protected final int maxChunksPerDocument;

    protected int listIndex = -1;
    private int chunkDocumentCounter = 1;

    AbstractChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat, List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        this.sourceDocument = sourceDocument;
        this.textSegments = textSegments;
        this.chunkConfig = chunkConfig;

        // Chunks cannot be written to the source document unless its format is JSON or XML. So if maxChunks is zero and
        // we don't have a JSON or XML document, all chunks will be written to a separate document.
        boolean cannotAddChunksToSourceDocument = !Format.JSON.equals(sourceDocumentFormat) && !Format.XML.equals(sourceDocumentFormat);
        this.maxChunksPerDocument = cannotAddChunksToSourceDocument && chunkConfig.getMaxChunks() == 0 ?
            textSegments.size() :
            chunkConfig.getMaxChunks();
    }

    protected abstract DocumentWriteOperation addChunksToSourceDocument();

    protected abstract DocumentWriteOperation makeChunkDocument();


    @Override
    public final boolean hasNext() {
        return listIndex < textSegments.size();
    }

    // Sonar complains that a NoSuchElementException should be thrown here, but that would only occur if the
    // hasNext() implementation has a bug, not if the user calls this too many times.
    @SuppressWarnings("java:S2272")
    @Override
    public DocumentWriteOperation next() {
        if (listIndex == -1) {
            listIndex++;
            if (this.maxChunksPerDocument == 0) {
                listIndex = textSegments.size();
                return addChunksToSourceDocument();
            }
            return sourceDocument;
        }

        DocumentWriteOperation writeOp = makeChunkDocument();
        chunkDocumentCounter++;
        return writeOp;
    }

    protected final String makeChunkDocumentUri(DocumentWriteOperation sourceDocument, String defaultUriSuffix) {
        if (chunkConfig.getUriPrefix() == null && chunkConfig.getUriSuffix() == null) {
            return String.format("%s-chunks-%d.%s", sourceDocument.getUri(), chunkDocumentCounter, defaultUriSuffix);
        }

        String uri = UUID.randomUUID().toString();
        if (chunkConfig.getUriPrefix() != null) {
            uri = chunkConfig.getUriPrefix() + uri;
        }
        if (chunkConfig.getUriSuffix() != null) {
            uri += chunkConfig.getUriSuffix();
        }
        return uri;
    }
}
