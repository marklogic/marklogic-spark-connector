/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.extraction.ExtractionResult;
import com.marklogic.spark.core.extraction.TextExtractor;
import com.marklogic.spark.core.nuclia.NuaClient;
import com.marklogic.spark.core.nuclia.NucliaDocumentProcessor;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Handles "processing" a document, which involves receiving a {@code DocumentInputs} instance, enriching it,
 * and returning one or more input instances.
 */
public class DocumentPipeline implements Closeable {

    private final TextExtractor textExtractor;
    private final TextSplitter textSplitter;
    private final TextClassifier textClassifier;
    private final EmbeddingProducer embeddingProducer;
    private final ChunkSelector chunkSelector;
    private final NuaClient nuaClient;
    private final NucliaDocumentProcessor nucliaProcessor;

    public DocumentPipeline(TextExtractor textExtractor, TextSplitter textSplitter, TextClassifier textClassifier, EmbeddingProducer embeddingProducer, ChunkSelector chunkSelector) {
        this.textExtractor = textExtractor;
        this.textSplitter = textSplitter;
        this.textClassifier = textClassifier;
        this.embeddingProducer = embeddingProducer;
        this.chunkSelector = chunkSelector;
        this.nuaClient = null;
        this.nucliaProcessor = null;
    }

    /**
     * Constructor for Nuclia-based pipeline. Nuclia handles extraction, splitting, and embedding generation.
     *
     * @param nuaClient      the Nuclia Understanding API client for processing
     * @param textClassifier optional text classifier (can be null)
     * @since 3.1.0
     */
    public DocumentPipeline(NuaClient nuaClient, TextClassifier textClassifier) {
        this.nuaClient = nuaClient;
        this.nucliaProcessor = new NucliaDocumentProcessor(nuaClient);
        this.textClassifier = textClassifier;
        this.textExtractor = null;
        this.textSplitter = null;
        this.embeddingProducer = null;
        this.chunkSelector = null;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(textClassifier);
        IOUtils.closeQuietly(nuaClient);
    }

    // Package-private getters for testing
    NuaClient getNuaClient() {
        return nuaClient;
    }

    TextClassifier getTextClassifier() {
        return textClassifier;
    }

    TextExtractor getTextExtractor() {
        return textExtractor;
    }

    TextSplitter getTextSplitter() {
        return textSplitter;
    }

    EmbeddingProducer getEmbeddingProducer() {
        return embeddingProducer;
    }

    ChunkSelector getChunkSelector() {
        return chunkSelector;
    }

    /**
     * Implements the pipeline for processing documents via text extraction, text splitting, text classification, and
     * embedding generation.
     */
    public void processDocuments(List<DocumentInputs> inputs) {
        if (nucliaProcessor != null) {
            processWithNuclia(inputs);
            return;
        }

        if (textExtractor != null) {
            inputs.stream().forEach(this::extractText);
        }

        if (textSplitter != null) {
            inputs.forEach(this::applySplitter);
        }

        if (textClassifier != null) {
            classifyText(inputs);
        }

        if (embeddingProducer != null) {
            addEmbeddings(inputs);
        }
    }

    private void processWithNuclia(List<DocumentInputs> inputs) {
        nucliaProcessor.processDocuments(inputs);

        // Optionally classify after Nuclia processing
        if (textClassifier != null) {
            classifyText(inputs);
        }
    }

    private void classifyText(List<DocumentInputs> inputs) {
        List<TextClassifier.ClassifiableContent> contents = new ArrayList<>();
        for (DocumentInputs input : inputs) {
            if (input.getContent() != null) {
                textClassifier.classifyDocument(input);
            }
            if (input.getChunkInputsList() != null) {
                for (ChunkInputs chunkInputs : input.getChunkInputsList()) {
                    contents.add(new ClassifiableChunk(chunkInputs));
                }
            }
        }

        textClassifier.classifyChunks(contents);
    }

    private void addEmbeddings(List<DocumentInputs> inputs) {
        List<Chunk> chunks = new ArrayList<>();
        for (DocumentInputs input : inputs) {
            if (input.getChunkInputsList() != null) {
                for (ChunkInputs chunkInputs : input.getChunkInputsList()) {
                    chunks.add(new EmbeddableChunk(chunkInputs));
                }
            } else if (chunkSelector != null) {
                DocumentAndChunks documentAndChunks = chunkSelector.selectChunks(input.getInitialUri(), input.getContent());
                if (documentAndChunks != null && documentAndChunks.hasChunks()) {
                    input.overrideContent(documentAndChunks.getContent());
                    documentAndChunks.getChunks().forEach(chunks::add);
                }
            }
        }
        if (!chunks.isEmpty()) {
            embeddingProducer.addEmbeddings(chunks);
        }
    }

    private record EmbeddableChunk(ChunkInputs chunkInputs) implements Chunk {

        @Override
        public String getEmbeddingText() {
            return chunkInputs.getText();
        }

        @Override
        public void addEmbedding(float[] embedding, String modelName) {
            chunkInputs.setEmbedding(embedding);
            chunkInputs.setModelName(modelName);
        }
    }

    private record ClassifiableChunk(ChunkInputs chunkInputs) implements TextClassifier.ClassifiableContent {

        @Override
        public String getTextToClassify() {
            return chunkInputs.getText();
        }

        @Override
        public void addClassification(byte[] classification) {
            chunkInputs.setClassification(classification);
        }
    }

    private void extractText(DocumentInputs inputs) {
        Optional<ExtractionResult> result = textExtractor.extractText(inputs);
        if (result.isPresent()) {
            inputs.setExtractedText(result.get().getText());
            inputs.setExtractedMetadata(result.get().getMetadata());
        }
    }

    private void applySplitter(DocumentInputs inputs) {
        List<String> chunks;
        if (inputs.getExtractedText() != null) {
            StringHandle content = new StringHandle(inputs.getExtractedText());
            chunks = textSplitter.split(inputs.getInitialUri(), content);
        } else {
            chunks = textSplitter.split(inputs.getInitialUri(), inputs.getContent());
        }
        inputs.setChunks(chunks);
    }
}
