/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.ChunkInputs;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.JsonChunk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class JsonChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ARRAY_NAME = "chunks";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final XmlMapper xmlMapper;

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                              List<ChunkInputs> chunkInputsList, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, chunkInputsList, chunkConfig);
        xmlMapper = new XmlMapper();
    }

    @Override
    protected DocumentWriteOperation addChunksToSourceDocument() {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) com.marklogic.spark.Util.getJsonFromHandle(content);

        ArrayNode chunksArray = doc.putArray(determineChunksArrayName(doc));
        List<Chunk> chunks = new ArrayList<>();
        for (ChunkInputs chunkInputs : chunkInputsList) {
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", chunkInputs.getText());
            if (chunkInputs.getClassification() != null) {
                try {
                    JsonNode classification = xmlMapper.readTree(chunkInputs.getClassification());
                    chunk.set("classification", classification);
                } catch (IOException e) {
                    throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceDocument.getUri(), e.getMessage()), e);
                }
            }
            var jsonChunk = new JsonChunk(chunk, null, chunkConfig.getEmbeddingName(), chunkConfig.isBase64EncodeVectors());
            if (chunkInputs.getEmbedding() != null) {
                jsonChunk.addEmbedding(chunkInputs.getEmbedding(), chunkInputs.getModelName());
            }
            chunks.add(jsonChunk);
        }

        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JacksonHandle(doc)),
            chunks
        );
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
            ChunkInputs chunkInputs = chunkInputsList.get(listIndex);
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", chunkInputs.getText());
            if (chunkInputs.getClassification() != null) {
                try {
                    JsonNode classification = xmlMapper.readTree(chunkInputs.getClassification());
                    chunk.set("classification", classification);
                } catch (IOException e) {
                    throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
                }
            }
            var jsonChunk = new JsonChunk(chunk, null, chunkConfig.getEmbeddingName(), chunkConfig.isBase64EncodeVectors());
            if (chunkInputs.getEmbedding() != null) {
                jsonChunk.addEmbedding(chunkInputs.getEmbedding(), chunkInputs.getModelName());
            }
            chunks.add(jsonChunk);
            listIndex++;
        }

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "json");
        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JacksonHandle(doc)),
            chunks
        );
    }

    private String determineChunksArrayName(ObjectNode doc) {
        return doc.has(DEFAULT_CHUNKS_ARRAY_NAME) ? "splitter-chunks" : DEFAULT_CHUNKS_ARRAY_NAME;
    }
}
