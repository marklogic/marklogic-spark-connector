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
import dev.langchain4j.azure.openai.spring.AutoConfig;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.model.azure.AzureOpenAiEmbeddingModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class EmbeddingTest extends AbstractIntegrationTest {

    @Autowired
    private Environment environment;

    @Test
    void test() {
        DocumentWriteOperation sourceDoc = readJsonDocument();
        SplitterDocumentProcessor splitter = newJsonSplitter("/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(sourceDoc);

        DocumentProcessor embedder = new EmbedderDocumentProcessor(
            new AllMiniLmL6V2QuantizedEmbeddingModel(), 2
        );
        docs.forEachRemaining(doc -> embedder.apply(doc));

        JsonNode doc = JsonUtil.getJsonFromHandle(sourceDoc.getContent());
        System.out.println(doc.toPrettyString());
    }

    /**
     * We could have both Java 11 and Java 17 uber jars for Azure support, where the Java 17 one uses
     * the Spring Boot integration to load config from a properties file.
     */
    @Test
    void azure() {
        DocumentWriteOperation sourceDoc = readJsonDocument();
        DocumentProcessor splitter = newJsonSplitter("/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(sourceDoc);

        EmbeddingModel embeddingModel = AzureOpenAiEmbeddingModel.builder()
            .apiKey(environment.getProperty("azureApiKey"))
            .deploymentName("text-test-embedding-ada-002")
            .endpoint("https://gpt-testing-custom-data1.openai.azure.com")
            .logRequestsAndResponses(true)
            .build();

        List<DocumentWriteOperation> output = new ArrayList<>();

        EmbedderDocumentProcessor embedder = new EmbedderDocumentProcessor(embeddingModel, 2);
        docs.forEachRemaining(doc -> embedder.apply(doc).forEachRemaining(output::add));
        embedder.get().forEachRemaining(output::add);

        output.forEach(doc -> {
            System.out.println(doc.getUri());
        });
    }

    @Test
    void azureSpringBoot() {
        DocumentWriteOperation sourceDoc = readJsonDocument();
        DocumentProcessor splitter = newJsonSplitter("/text");
        Iterator<DocumentWriteOperation> docs = splitter.apply(sourceDoc);

        List<DocumentWriteOperation> output = new ArrayList<>();

        ConfigurableApplicationContext context = SpringApplication.run(AutoConfig.class);
        EmbeddingModel azureEmbeddingModel = context.getBean(EmbeddingModel.class);

        EmbedderDocumentProcessor embedder = new EmbedderDocumentProcessor(azureEmbeddingModel, 2);
        docs.forEachRemaining(doc -> embedder.apply(doc).forEachRemaining(output::add));
        embedder.get().forEachRemaining(output::add);

        output.forEach(doc -> System.out.println(doc.getUri()));
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
}
