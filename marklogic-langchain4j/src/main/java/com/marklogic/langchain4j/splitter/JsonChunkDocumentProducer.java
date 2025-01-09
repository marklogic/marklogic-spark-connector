/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.langchain4j.Util;
import com.marklogic.langchain4j.embedding.Chunk;
import com.marklogic.langchain4j.embedding.DocumentAndChunks;
import com.marklogic.langchain4j.embedding.JsonChunk;
import com.smartlogic.classificationserver.client.ClassificationScore;
import dev.langchain4j.data.segment.TextSegment;

import java.util.*;

class JsonChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ARRAY_NAME = "chunks";

    private final ObjectMapper objectMapper = new ObjectMapper();

    JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                              List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
    }

    @Override
    protected DocumentWriteOperation addChunksToSourceDocument() {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) Util.getJsonFromHandle(content);

        ArrayNode chunksArray = doc.putArray(determineChunksArrayName(doc));
        List<Chunk> chunks = new ArrayList<>();
        textSegments.forEach(textSegment -> {
            String text = textSegment.text();
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
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
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            String text = textSegments.get(listIndex++).text();
            ObjectNode chunk = chunksArray.addObject();
            chunk.put("text", text);
            if (chunkConfig.getClassifier() != null) {
                chunk.set("concepts", generateConceptsForChunk(text));
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

    private ArrayNode generateConceptsForChunk(String text) {
        Map<String, Collection<ClassificationScore>> classificationScores = chunkConfig.getClassifier().classifyText(text);
        ArrayNode conceptsArray = objectMapper.createArrayNode();
        for (Map.Entry<String, Collection<ClassificationScore>> entry : classificationScores.entrySet()) {
            for (ClassificationScore classificationScore : classificationScores.get(entry.getKey())) {
                ObjectNode concept = conceptsArray.addObject();
                concept.put("id", classificationScore.getId());
                concept.put("name", entry.getKey());
                concept.put("value", classificationScore.getName());
                concept.put("score", classificationScore.getScore());
            }
        }
        return conceptsArray;
    }
}
