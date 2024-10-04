/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class ChunkDocumentChunkProcessor implements ChunkProcessor {

    private final String documentType;

    // Could put these 3 things into a class.
    private final String rootName;
    private final String namespace;
    private final DocumentMetadataHandle metadata;

    public ChunkDocumentChunkProcessor(ContextSupport context) {
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
    }

    @Override
    public Iterator<DocumentWriteOperation> processChunks(DocumentWriteOperation sourceDocument, List<TextSegment> chunks) {
        return new JsonChunkProducer(sourceDocument, chunks, rootName, metadata);
    }

    private static class JsonChunkProducer implements Iterator<DocumentWriteOperation> {

        private final DocumentWriteOperation sourceDocument;
        private final List<TextSegment> chunks;
        private final String rootName;
        private final DocumentMetadataHandle metadata;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private int chunkIndex = -1;

        public JsonChunkProducer(DocumentWriteOperation sourceDocument, List<TextSegment> chunks, String rootName, DocumentMetadataHandle metadata) {
            this.sourceDocument = sourceDocument;
            this.chunks = chunks;
            this.rootName = rootName;
            this.metadata = metadata;
        }

        @Override
        public boolean hasNext() {
            return chunkIndex < chunks.size();
        }

        @Override
        public DocumentWriteOperation next() {
            if (chunkIndex == -1) {
                chunkIndex++;
                return sourceDocument;
            }

            ObjectNode doc = objectMapper.createObjectNode();
            ObjectNode root = doc;
            if (rootName != null) {
                root = doc.putObject(rootName);
            }

            // Interesting. What do we call the URI?
            // Probably need this to be configurable.
            // To guarantee uniqueness, it either needs a UUID with prefix/suffix options, or it should be based
            // off the source document URI.
            // If we do options... hmm...
            // spark.marklogic.write.splitter.output.uriPrefix
            // spark.marklogic.write.splitter.output.uriSuffix
            // If neither is specified, then the source URI will be used. I don't think we need to bother with chopping
            // off the extension. Just do "-chunk-number.json".
            // The above should work for a sidecar and for chunks, with chunks just adding the index of each text
            // segment.
            String uri = sourceDocument.getUri() + "-chunk-" + chunkIndex + ".json";
            root.put("source-uri", sourceDocument.getUri());
            root.put("text", chunks.get(chunkIndex).text());
            chunkIndex++;
            return new DocumentWriteOperationImpl(uri, metadata, new JacksonHandle(doc));
        }
    }
}
