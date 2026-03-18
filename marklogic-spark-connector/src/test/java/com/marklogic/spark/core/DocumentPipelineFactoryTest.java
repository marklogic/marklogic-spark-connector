/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;

class DocumentPipelineFactoryTest {

    @Test
    void nucliaWithAllRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_NUCLIA_NUA_KEY, "test-api-key");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNotNull(pipeline.getNuaClient(), "NuaClient should be present");
        assertEquals("https://aws-us-east-2-1.rag.progress.cloud/api/v1", pipeline.getNuaClient().getBaseUrl());
        assertEquals(120, pipeline.getNuaClient().getTimeoutSeconds(), "Default timeout should be 120 seconds");

        assertNull(pipeline.getTextExtractor(), "TextExtractor should not be present in Nuclia pipeline");
        assertNull(pipeline.getTextSplitter(), "TextSplitter should not be present in Nuclia pipeline");
        assertNull(pipeline.getEmbeddingProducer(), "EmbeddingProducer should not be present in Nuclia pipeline");
        assertNull(pipeline.getChunkSelector(), "ChunkSelector should not be present in Nuclia pipeline");
    }

    @Test
    void nucliaWithCustomTimeout() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_NUCLIA_NUA_KEY, "test-api-key");
        options.put(Options.WRITE_NUCLIA_TIMEOUT, "300");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNotNull(pipeline.getNuaClient());
        assertEquals(300, pipeline.getNuaClient().getTimeoutSeconds());
    }

    @Test
    void nucliaWithEmptyNuaKey() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_NUCLIA_NUA_KEY, "   ");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNull(pipeline, "Pipeline should be null when NUA key is empty/whitespace");
    }

    @Test
    void noOptionsReturnsNull() {
        Map<String, String> options = new HashMap<>();
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNull(pipeline, "Pipeline should be null when no processing options are provided");
    }

    @Test
    void textExtractorOnly() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_EXTRACTED_TEXT, "true");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNotNull(pipeline, "Pipeline should be created with text extractor");
        assertNotNull(pipeline.getTextExtractor(), "TextExtractor should be present");
        assertNull(pipeline.getNuaClient(), "NuaClient should not be present");
        assertNull(pipeline.getTextSplitter(), "TextSplitter should not be present");
        assertNull(pipeline.getEmbeddingProducer(), "EmbeddingProducer should not be present");
    }

    @Test
    void nucliaWithClassifier() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.WRITE_NUCLIA_NUA_KEY, "test-api-key");
        options.put(Options.WRITE_CLASSIFIER_HOST, "classifier-host");
        options.put(Options.WRITE_CLASSIFIER_PORT, "8080");
        options.put(Options.WRITE_CLASSIFIER_PATH, "/classify");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNotNull(pipeline, "Pipeline should be created with Nuclia and classifier");
        assertNotNull(pipeline.getNuaClient(), "NuaClient should be present");
        assertNotNull(pipeline.getTextClassifier(), "TextClassifier should be present even with Nuclia");
    }

    @Test
    void nucliaHasPriorityOverStandardPipeline() {
        Map<String, String> options = new HashMap<>();
        // Nuclia options
        options.put(Options.WRITE_NUCLIA_NUA_KEY, "test-api-key");
        // Standard pipeline options (should be ignored)
        options.put(Options.WRITE_EXTRACTED_TEXT, "true");
        options.put(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, "1000");
        Context context = new Context(options);

        DocumentPipeline pipeline = DocumentPipelineFactory.newDocumentPipeline(context);

        assertNotNull(pipeline, "Pipeline should be created");
        assertNotNull(pipeline.getNuaClient(), "NuaClient should be present");
        assertNull(pipeline.getTextExtractor(), "TextExtractor should be ignored when Nuclia is configured");
        assertNull(pipeline.getTextSplitter(), "TextSplitter should be ignored when Nuclia is configured");
    }
}
