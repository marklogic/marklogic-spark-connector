/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.writer.JsonUtil;
import com.marklogic.spark.writer.XmlUtil;
import com.marklogic.spark.writer.splitter.*;
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

    @Test
    void customizedPaths() {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.putObject("custom").putArray("custom-chunks").addObject().putObject("wrapper").put("custom-text", "Hello world");

        EmbedderDocumentProcessor embedder = new EmbedderDocumentProcessor(
            new JsonChunkSelector.Builder()
                .withChunksPointer("/custom/custom-chunks")
                .withTextPointer("/wrapper/custom-text")
                .withEmbeddingArrayName("custom-embedding")
                .build(),
            new AllMiniLmL6V2EmbeddingModel()
        );

        DocumentWriteOperation output = embedder.apply(new DocumentWriteOperationImpl("a.json", null, new JacksonHandle(doc))).next();
        JsonNode outputDoc = JsonUtil.getJsonFromHandle(output.getContent());

        assertEquals("Hello world", outputDoc.at("/custom/custom-chunks/0/wrapper/custom-text").asText());
        JsonNode chunk = outputDoc.get("custom").get("custom-chunks").get(0);
        assertTrue(chunk.has("custom-embedding"));
        assertEquals(JsonNodeType.ARRAY, chunk.get("custom-embedding").getNodeType());
    }

    @Test
    void xml() {
        SplitterDocumentProcessor splitter = newXmlSplitter(500, 2, "/node()/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(readXmlDocument());

        // Skip the source document.
        docs.next();

        docs.forEachRemaining(doc -> {
            XmlNode node = new XmlNode(XmlUtil.extractDocument(doc.getContent()));
            node.assertElementCount("/root/chunks/chunk", 2);
            node.assertElementExists("/root/chunks/chunk[1]/embedding");
            node.assertElementExists("/root/chunks/chunk[2]/embedding");
        });
    }

    private DocumentWriteOperation readJsonDocument() {
        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }

    private DocumentWriteOperation readXmlDocument() {
        final String uri = "/marklogic-docs/java-client-intro.xml";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JDOMHandle contentHandle = getDatabaseClient().newXMLDocumentManager().read(uri, metadata, new JDOMHandle());
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

    private SplitterDocumentProcessor newXmlSplitter(int maxChunkSize, int maxChunks, String xpath) {
        return new SplitterDocumentProcessor(
            new DOMTextSelector(xpath, null),
            DocumentSplitters.recursive(maxChunkSize, 0),
            new DefaultChunkAssembler(
                new ChunkConfig.Builder().withMaxChunks(maxChunks).build(),
                new EmbeddingGenerator(new AllMiniLmL6V2EmbeddingModel())
            )
        );
    }
}
