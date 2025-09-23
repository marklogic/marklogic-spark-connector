/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.Format;
import com.marklogic.spark.Util;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class DefaultChunkAssembler implements ChunkAssembler {

    private final ChunkConfig chunkConfig;

    public DefaultChunkAssembler(ChunkConfig chunkConfig) {
        this.chunkConfig = chunkConfig;
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<String> textSegments, List<byte[]> classifications, List<float[]> embeddings) {
        final Format sourceDocumentFormat = Util.determineSourceDocumentFormat(sourceDocument.getContent(), sourceDocument.getUri());
        if (sourceDocumentFormat == null) {
            Util.MAIN_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        final Format chunkDocumentFormat = determineChunkDocumentFormat(sourceDocumentFormat);

        return Format.XML.equals(chunkDocumentFormat) ?
            new XmlChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications, embeddings) :
            new JsonChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications, embeddings);
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
