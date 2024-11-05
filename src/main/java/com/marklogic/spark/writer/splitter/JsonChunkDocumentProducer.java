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
import com.marklogic.spark.writer.embedding.Chunk;
import com.marklogic.spark.writer.embedding.EmbeddingGenerator;
import com.marklogic.spark.writer.embedding.JsonChunk;
import dev.langchain4j.data.segment.TextSegment;

import java.util.ArrayList;
import java.util.List;

class JsonChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ARRAY_NAME = "chunks";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final EmbeddingGenerator embeddingGenerator;

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                              List<TextSegment> textSegments, ChunkConfig chunkConfig, EmbeddingGenerator embeddingGenerator) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
        this.embeddingGenerator = embeddingGenerator;
    }

    @Override
    protected DocumentWriteOperation addChunksToSourceDocument() {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) JsonUtil.getJsonFromHandle(content);

        ArrayNode chunksArray = doc.putArray(determineChunksArrayName(doc));
        List<Chunk> chunks = new ArrayList<>();
        textSegments.forEach(textSegment -> {
            String text = textSegment.text();
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
            chunks.add(new JsonChunk(sourceDocument.getUri(), chunk));
        });
        addEmbeddingsToChunks(chunks);

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

        ArrayNode chunksArray = rootField.putArray(DEFAULT_CHUNKS_ARRAY_NAME);
        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            String text = textSegments.get(listIndex++).text();
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
            chunks.add(new JsonChunk(sourceDocument.getUri(), chunk));
        }
        addEmbeddingsToChunks(chunks);

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "json");
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JacksonHandle(doc));
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        if (this.embeddingGenerator != null) {
            this.embeddingGenerator.addEmbeddings(chunks);
        }
    }

    private String determineChunksArrayName(ObjectNode doc) {
        return doc.has(DEFAULT_CHUNKS_ARRAY_NAME) ? "splitter-chunks" : DEFAULT_CHUNKS_ARRAY_NAME;
    }
}
