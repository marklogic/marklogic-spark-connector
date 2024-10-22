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
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.DocumentProcessor;
import com.marklogic.spark.writer.JsonUtil;
import com.marklogic.spark.writer.splitter.ChunkConfig;
import com.marklogic.spark.writer.splitter.DefaultChunkAssembler;
import com.marklogic.spark.writer.splitter.JsonPointerTextSelector;
import com.marklogic.spark.writer.splitter.SplitterDocumentProcessor;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.util.Iterator;

class EmbeddingTest extends AbstractIntegrationTest {

    @Test
    void test() {
        DocumentWriteOperation sourceDoc = readJsonDocument();
        SplitterDocumentProcessor splitter = newJsonSplitter("/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(sourceDoc);

        DocumentProcessor embedder = new EmbedderDocumentProcessor(
            new AllMiniLmL6V2EmbeddingModel(), 2
        );
        docs.forEachRemaining(doc -> embedder.apply(doc));

        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDoc.getContent());
        System.out.println(doc.toPrettyString());
    }

    @Test
    void realTest() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 2)
            .option(Options.WRITE_EMBEDDER_BATCH_SIZE, 1)
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction")
            .mode(SaveMode.Append)
            .save();
    }

    private DocumentWriteOperation readJsonDocument() {
        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }

    private SplitterDocumentProcessor newJsonSplitter(String... jsonPointers) {
        return new SplitterDocumentProcessor(
            new JsonPointerTextSelector(jsonPointers, null),
            DocumentSplitters.recursive(500, 0),
            new DefaultChunkAssembler(new ChunkConfig.Builder()
                .withMaxChunks(2)
                // TODO The rootName screws up the default path.
                .build())
        );
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
