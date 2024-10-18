/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.writer.DocumentProcessor;
import com.marklogic.spark.writer.JsonUtil;
import com.marklogic.spark.writer.splitter.ChunkConfig;
import com.marklogic.spark.writer.splitter.DefaultChunkAssembler;
import com.marklogic.spark.writer.splitter.JsonPointerTextSelector;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessor;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

class EmbeddingTest extends AbstractIntegrationTest {

    @Test
    void test() {
        DocumentWriteOperation sourceDoc = readJsonDocument();
        DocumentProcessor splitter = newJsonSplitter("/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(sourceDoc);

        // Could make this batch up requests?
        DocumentProcessor embedder = new EmbeddingDocumentProcessor(
            new AllMiniLmL6V2EmbeddingModel()
        );
        docs.forEachRemaining(doc -> embedder.apply(doc));

        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDoc.getContent());
        System.out.println(doc.toPrettyString());
    }

    private DocumentWriteOperation readJsonDocument() {
        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }

    private DocumentProcessor newJsonSplitter(String... jsonPointers) {
        return new SplitterDocumentProcessor(
            new JsonPointerTextSelector(jsonPointers, null),
            DocumentSplitters.recursive(500, 0),
            new DefaultChunkAssembler(new ChunkConfig.Builder().build())
        );
    }
}
