/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.BaseHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.Util;
import com.marklogic.spark.writer.JsonUtil;
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
        final Format format = determineSourceDocumentFormat(sourceDocument);
        if (format == null) {
            Util.MAIN_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        // Will refactor this soon so that it's all in the separate class.
        if (Format.JSON.equals(format)) {
            if (chunkConfig.getMaxChunks() > 0) {
                return new JsonChunkDocumentProducer(sourceDocument, textSegments, chunkConfig);
            }
            return addChunksToJsonDocument(sourceDocument, textSegments);
        }

        return new XmlChunkDocumentProducer(sourceDocument, format, textSegments, chunkConfig);
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

    private Iterator<DocumentWriteOperation> addChunksToJsonDocument(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) JsonUtil.getJsonFromHandle(content);

        ArrayNode chunks = doc.putArray("chunks");
        textSegments.forEach(textSegment -> chunks.addObject().put("text", textSegment.text()));

        DocumentWriteOperation result = new DocumentWriteOperationImpl(sourceDocument.getUri(),
            sourceDocument.getMetadata(), new JacksonHandle(doc));

        return Stream.of(result).iterator();
    }
}
