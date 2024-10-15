/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.writer.JsonUtil;
import dev.langchain4j.data.segment.TextSegment;

import java.util.List;

class JsonChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat, List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
    }

    @Override
    protected DocumentWriteOperation addChunksToSourceDocument() {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) JsonUtil.getJsonFromHandle(content);

        ArrayNode chunks = doc.putArray("chunks");
        textSegments.forEach(textSegment -> {
            String text = textSegment.text();
            chunks.addObject().put("text", text);
        });

        return new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JacksonHandle(doc));
    }

    @Override
    protected DocumentWriteOperation makeChunkDocument() {
        ObjectNode doc = objectMapper.createObjectNode();
        ObjectNode rootField = doc;
        if (chunkConfig.getRootName() != null) {
            rootField = doc.putObject(chunkConfig.getRootName());
        }
        String uri = sourceDocument.getUri();
        rootField.put("source-uri", uri);

        ArrayNode chunks = rootField.putArray("chunks");
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            String text = textSegments.get(listIndex++).text();
            chunks.addObject().put("text", text);
        }

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "json");
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JacksonHandle(doc));
    }
}
