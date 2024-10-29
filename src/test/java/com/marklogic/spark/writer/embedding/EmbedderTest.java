/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.writer.JsonUtil;
import com.marklogic.spark.writer.splitter.ChunkConfig;
import com.marklogic.spark.writer.splitter.DefaultChunkAssembler;
import com.marklogic.spark.writer.splitter.JsonPointerTextSelector;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessor;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the embedder without writing documents to MarkLogic and without using Spark.
 */
class EmbedderTest extends AbstractIntegrationTest {

    @Test
    void defaultPaths() {
        SplitterDocumentProcessor splitter = newJsonSplitter(500, 2, "/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(readJsonDocument());

        // Skip the first doc, which is the source document that doesn't have any chunks.
        docs.next();

        docs.forEachRemaining(doc -> {
            JsonNode node = JsonUtil.getJsonFromHandle(doc.getContent());
            ArrayNode chunks = (ArrayNode) node.get("chunks");
            assertEquals(2, chunks.size());
            for (JsonNode chunk : chunks) {
                assertTrue(chunk.has("text"));
                assertTrue(chunk.has("embedding"));
                assertEquals(JsonNodeType.ARRAY, chunk.get("embedding").getNodeType());
            }
        });
    }

    private DocumentWriteOperation readJsonDocument() {
        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }

    private SplitterDocumentProcessor newJsonSplitter(int maxChunkSize, int maxChunks, String... jsonPointers) {
        return new SplitterDocumentProcessor(
            new JsonPointerTextSelector(jsonPointers, null),
            DocumentSplitters.recursive(maxChunkSize, 0),
            new DefaultChunkAssembler(
                new ChunkConfig.Builder().withMaxChunks(maxChunks).build(),
                new EmbeddingGenerator(new AllMiniLmL6V2EmbeddingModel())
            )
        );
    }
}
