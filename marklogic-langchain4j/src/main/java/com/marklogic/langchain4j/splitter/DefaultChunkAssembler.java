/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.Format;
import com.marklogic.langchain4j.Util;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.marklogic.langchain4j.Util.determineSourceDocumentFormat;

public class DefaultChunkAssembler implements ChunkAssembler {

    private final ChunkConfig chunkConfig;

    public DefaultChunkAssembler(ChunkConfig chunkConfig) {
        this.chunkConfig = chunkConfig;
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleStringChunks(DocumentWriteOperation sourceDocument, List<String> chunks) {
        Metadata metadata = new Metadata();
        List<TextSegment> textSegments = chunks.stream().map(chunk -> new TextSegment(chunk, metadata)).collect(Collectors.toList());
        return assembleChunks(sourceDocument, textSegments);
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        final Format sourceDocumentFormat = determineSourceDocumentFormat(sourceDocument.getContent(), sourceDocument.getUri());
        if (sourceDocumentFormat == null) {
            Util.LANGCHAIN4J_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        final Format chunkDocumentFormat = determineChunkDocumentFormat(sourceDocumentFormat);

        return Format.XML.equals(chunkDocumentFormat) ?
            new XmlChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig) :
            new JsonChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
    }

    private Format determineChunkDocumentFormat(Format sourceDocumentFormat) {
        final boolean canAddChunksToSourceDocument = Format.XML.equals(sourceDocumentFormat) || Format.JSON.equals(sourceDocumentFormat);
        if (canAddChunksToSourceDocument && chunkConfig.getMaxChunks() == 0) {
            return sourceDocumentFormat;
        }

        if (chunkConfig.getDocumentType() != null || !canAddChunksToSourceDocument) {
            return "xml".equalsIgnoreCase(chunkConfig.getDocumentType()) ? Format.XML : Format.JSON;
        }

        return sourceDocumentFormat;
    }
}
