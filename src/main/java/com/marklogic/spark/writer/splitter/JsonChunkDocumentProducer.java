/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.JacksonHandle;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

class JsonChunkDocumentProducer implements Iterator<DocumentWriteOperation> {

    private final DocumentWriteOperation sourceDocument;
    private final List<TextSegment> textSegments;
    private final ChunkConfig chunkConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private int listIndex = -1;
    private int counter;

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        this.sourceDocument = sourceDocument;
        this.textSegments = textSegments;
        this.chunkConfig = chunkConfig;
    }

    @Override
    public boolean hasNext() {
        return listIndex < textSegments.size();
    }

    // Sonar complains that a NoSuchElementException should be thrown here, but that would only occur if the
    // hasNext() implementation has a bug, not if the user calls this too many times.
    @SuppressWarnings("java:S2272")
    @Override
    public DocumentWriteOperation next() {
        if (listIndex == -1) {
            listIndex++;
            return sourceDocument;
        }

        ObjectNode doc = objectMapper.createObjectNode();
        ObjectNode rootElement = doc;
        if (chunkConfig.getRootName() != null) {
            rootElement = doc.putObject(chunkConfig.getRootName());
        }
        rootElement.put("source-uri", sourceDocument.getUri());
        ArrayNode chunks = rootElement.putArray("chunks");
        for (int i = 0; i < chunkConfig.getMaxChunks() && hasNext(); i++) {
            chunks.addObject().put("text", textSegments.get(listIndex++).text());
        }

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument);
        counter++;
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JacksonHandle(doc));
    }

    private String makeChunkDocumentUri(DocumentWriteOperation sourceDocument) {
        if (chunkConfig.getUriPrefix() == null && chunkConfig.getUriSuffix() == null) {
            return String.format("%s-chunks-%d.json", sourceDocument.getUri(), counter);
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
