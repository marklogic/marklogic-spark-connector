/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.io.BaseHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.langchain4j.Util;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class DefaultChunkAssembler implements ChunkAssembler {

    private final ChunkConfig chunkConfig;

    public DefaultChunkAssembler(ChunkConfig chunkConfig) {
        this.chunkConfig = chunkConfig;
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        final Format sourceDocumentFormat = determineSourceDocumentFormat(sourceDocument);
        if (sourceDocumentFormat == null) {
            Util.LANGCHAIN4J_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        final Format chunkDocumentFormat = determineChunkDocumentFormat(sourceDocumentFormat);

        return Format.XML.equals(chunkDocumentFormat) ?
            new XmlChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig) :
            new JsonChunkDocumentProducer(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
    }

    private Format determineSourceDocumentFormat(DocumentWriteOperation sourceDocument) {
        final AbstractWriteHandle content = sourceDocument.getContent();
        final String uri = sourceDocument.getUri() != null ? sourceDocument.getUri() : "";
        if (content instanceof JacksonHandle || uri.endsWith(".json")) {
            return Format.JSON;
        }
        if (content instanceof DOMHandle || content instanceof JDOMHandle || uri.endsWith(".xml")) {
            return Format.XML;
        }
        if (content instanceof BaseHandle) {
            return ((BaseHandle) content).getFormat();
        }
        return null;
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
