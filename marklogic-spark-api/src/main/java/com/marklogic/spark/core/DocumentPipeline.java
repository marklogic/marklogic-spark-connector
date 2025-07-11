/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
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
import com.marklogic.spark.core.splitter.TextSplitter;

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

    public DocumentPipeline(TextExtractor textExtractor, TextSplitter textSplitter, TextClassifier textClassifier, EmbeddingProducer embeddingProducer, ChunkSelector chunkSelector) {
        this.textExtractor = textExtractor;
        this.textSplitter = textSplitter;
        this.textClassifier = textClassifier;
        this.embeddingProducer = embeddingProducer;
        this.chunkSelector = chunkSelector;
    }

    @Override
    public void close() throws IOException {
        if (textClassifier != null) {
            textClassifier.close();
        }
    }

    /**
     * Implements the pipeline for processing documents via text extraction, text splitting, text classification, and
     * embedding generation.
     */
    public void processDocuments(List<DocumentInputs> inputs) {
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

    private void classifyText(List<DocumentInputs> inputs) {
        List<TextClassifier.ClassifiableContent> contents = new ArrayList<>();
        for (DocumentInputs input : inputs) {
            if (input.getContent() != null) {
                textClassifier.classifyDocument(input);
            }
            if (input.getChunks() != null) {
                for (int i = 0; i < input.getChunks().size(); i++) {
                    contents.add(new ClassifiableChunk(input, i));
                }
            }
        }

        textClassifier.classifyChunks(contents);
    }

    private void addEmbeddings(List<DocumentInputs> inputs) {
        List<Chunk> chunks = new ArrayList<>();
        for (DocumentInputs input : inputs) {
            if (input.getChunks() != null) {
                for (int i = 0; i < input.getChunks().size(); i++) {
                    chunks.add(new EmbeddableChunk(input, i));
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

    private static class EmbeddableChunk implements Chunk {
        private final DocumentInputs inputs;
        private final int chunkIndex;

        public EmbeddableChunk(DocumentInputs inputs, int chunkIndex) {
            this.inputs = inputs;
            this.chunkIndex = chunkIndex;
        }

        @Override
        public String getEmbeddingText() {
            return inputs.getChunks().get(chunkIndex);
        }

        @Override
        public void addEmbedding(float[] embedding) {
            inputs.addEmbedding(embedding);
        }
    }

    private static class ClassifiableChunk implements TextClassifier.ClassifiableContent {
        private final int chunkListIndex;
        private final DocumentInputs documentInputs;

        private ClassifiableChunk(DocumentInputs documentInputs, int chunkListIndex) {
            this.documentInputs = documentInputs;
            this.chunkListIndex = chunkListIndex;
        }

        @Override
        public String getTextToClassify() {
            return documentInputs.getChunks().get(chunkListIndex);
        }

        @Override
        public void addClassification(byte[] classification) {
            // We shouldn't need an index here, as we expect to add classifications in the correct order, based on
            // classifying chunks in the order defined by the chunks list.
            documentInputs.addChunkClassification(classification);
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
