/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
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
import com.marklogic.spark.core.classifier.SemaphoreUtil;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.JsonChunk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class JsonChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ARRAY_NAME = "chunks";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final XmlMapper xmlMapper;

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                              List<String> textSegments, ChunkConfig chunkConfig, List<byte[]> classifications) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications);
        xmlMapper = new XmlMapper();
    }

    @Override
    protected DocumentWriteOperation addChunksToSourceDocument() {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) com.marklogic.spark.Util.getJsonFromHandle(content);

        ArrayNode chunksArray = doc.putArray(determineChunksArrayName(doc));
        List<Chunk> chunks = new ArrayList<>();
        AtomicInteger ct = new AtomicInteger(0);
        textSegments.forEach(textSegment -> {
            String text = textSegment;
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
            if (classifications != null && !classifications.isEmpty()) {
                try {
                    JsonNode classificationNode = xmlMapper.readTree(classifications.get(ct.getAndIncrement()))
                        .get(SemaphoreUtil.CLASSIFICATION_MAIN_ELEMENT);
                    chunk.set("classification", classificationNode);
                } catch (IOException e) {
                    throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceDocument.getUri(), e.getMessage()), e);
                }
            }
            chunks.add(new JsonChunk(sourceDocument.getUri(), chunk));
        });

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
        AtomicInteger ct = new AtomicInteger(0);
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            String text = textSegments.get(listIndex++);
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
            if (classifications != null) {
                try {
                    JsonNode classificationNode = xmlMapper.readTree(classifications.get(ct.getAndIncrement()))
                        .get(SemaphoreUtil.CLASSIFICATION_MAIN_ELEMENT);
                    chunk.set("classification", classificationNode);
                } catch (IOException e) {
                    throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
                }
            }
            chunks.add(new JsonChunk(sourceDocument.getUri(), chunk));
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
