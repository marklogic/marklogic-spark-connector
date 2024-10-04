/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import dev.langchain4j.data.segment.TextSegment;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * For this and "chunk" document, we don't need to know anything about the source document since we're not going to
 * touch it.
 */
public class SidecarDocumentChunkProcessor implements ChunkProcessor {

    private final String documentType;
    private final String rootName;
    private final String namespace;
    private final DocumentMetadataHandle metadata;

    private final ObjectMapper objectMapper;

    public SidecarDocumentChunkProcessor(ContextSupport context) {
        this.documentType = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_DOCUMENT_TYPE);
        this.rootName = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_ROOT_NAME);
        this.namespace = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_NAMESPACE);

        this.metadata = new DocumentMetadataHandle();
        if (context.hasOption(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS)) {
            String[] collections = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS).split(",");
            this.metadata.getCollections().addAll(collections);
        }
        if (context.hasOption(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS)) {
            String permissions = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS);
            this.metadata.getPermissions().addFromDelimitedString(permissions);
        } else if (context.hasOption(Options.WRITE_PERMISSIONS)) {
            String permissions = context.getStringOption(Options.WRITE_PERMISSIONS);
            this.metadata.getPermissions().addFromDelimitedString(permissions);
        }

        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Iterator<DocumentWriteOperation> processChunks(DocumentWriteOperation sourceDocument, List<TextSegment> chunks) {
        // Just doing JSON for now.
        ObjectNode doc = objectMapper.createObjectNode();
        ObjectNode root = doc;
        if (this.rootName != null) {
            root = doc.putObject(rootName);
        }
        root.put("source-uri", sourceDocument.getUri());
        ArrayNode chunksArray = root.putArray("chunks");
        chunks.forEach(chunk -> chunksArray.addObject().put("text", chunk.text()));

        String uri = sourceDocument.getUri() + "-chunks.json";
        return Stream.of(sourceDocument, new DocumentWriteOperationImpl(uri, metadata, new JacksonHandle(doc))).iterator();
    }
}
