/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

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
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<String> textSegments, List<byte[]> classifications) {
        final Format sourceDocumentFormat = com.marklogic.spark.Util.determineSourceDocumentFormat(sourceDocument.getContent(), sourceDocument.getUri());
        if (sourceDocumentFormat == null) {
            Util.MAIN_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        final Format chunkDocumentFormat = determineChunkDocumentFormat(sourceDocumentFormat);

        return Format.XML.equals(chunkDocumentFormat) ?
            new XmlChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications) :
            new JsonChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications);
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
