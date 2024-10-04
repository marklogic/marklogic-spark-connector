/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.writer.JsonUtil;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Default impl that adds chunks to the source document.
 */
public class SourceDocumentChunkProcessor implements ChunkProcessor {

    /**
     * Just doing JSON for now.
     *
     * What if the user has a byte array? And the format column isn't populated? I think we need the user to tell us
     * what kind of documents they have.
     *
     * I think we can force the user to specify "WRITE_DOCUMENT_TYPE". If they don't, this could try JSON first, then
     * XML, then throw an error.
     *
     * @return
     */
    @Override
    public Iterator<DocumentWriteOperation> processChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        ObjectNode content = (ObjectNode) JsonUtil.getJsonFromHandle(sourceDocument.getContent());

        // Would want to throw an error if this exists already - or possibly add to it?
        ArrayNode chunks = content.putArray("chunks");
        textSegments.forEach(textSegment -> chunks.addObject().put("text", textSegment.text()));

        return sourceDocument.getContent() instanceof JacksonHandle ?
            Stream.of(sourceDocument).iterator() :

            Stream.of((DocumentWriteOperation) new DocumentWriteOperationImpl(
                sourceDocument.getUri(),
                sourceDocument.getMetadata(),
                new JacksonHandle(content)
            )).iterator();
    }
}
