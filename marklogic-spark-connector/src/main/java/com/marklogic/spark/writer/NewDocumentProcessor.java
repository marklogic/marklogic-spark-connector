/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This will soon handle extracting, splitting, classifying, and embedding.
 * And ideally, it and DocBuilder can all be moved to the marklogic-spark-api project where
 * it can be more easily reused in the future.
 */
public class NewDocumentProcessor {

    private final Tika tika;
    private final TextSplitter textSplitter;
    private final TextClassifier textClassifier;
    private final EmbeddingProducer embeddingProducer;
    private final ChunkSelector chunkSelector;

    public NewDocumentProcessor(Tika tika, TextSplitter textSplitter, TextClassifier textClassifier,
                                EmbeddingProducer embeddingProducer, ChunkSelector chunkSelector) {
        this.tika = tika;
        this.textSplitter = textSplitter;
        this.textClassifier = textClassifier;
        this.embeddingProducer = embeddingProducer;
        this.chunkSelector = chunkSelector;
    }

    public void processDocument(DocumentInputs inputs) {
        final String uri = inputs.getInitialUri();
        if (tika != null && inputs.getContent() instanceof BytesHandle) {
            extractText(inputs);
        }
        if (textSplitter != null) {
            applySplitter(inputs);
        }

        // Not sure if we should run the classifier if the user specifies a splitter. We may need options to control
        // whether this happens or not.
        if (textClassifier != null) {
            if (inputs.getExtractedText() != null) {
                byte[] classification = textClassifier.classifyTextToBytes(uri, inputs.getExtractedText());
                inputs.setClassificationResponse(classification);
            } else {
                String content = HandleAccessor.contentAsString(inputs.getContent());
                byte[] classification = textClassifier.classifyTextToBytes(uri, content);
                inputs.setClassificationResponse(classification);
            }
        }

        if (embeddingProducer != null) {
            if (inputs.getChunks() != null && !inputs.getChunks().isEmpty()) {
                addEmbeddingsToSplitterChunks(inputs);
            } else if (chunkSelector != null) {
                addEmbeddingsToExistingChunks(inputs);
            }
        }
    }

    private void extractText(DocumentInputs documentInputs) {
        BytesHandle content = (BytesHandle) documentInputs.getContent();
        try (ByteArrayInputStream stream = new ByteArrayInputStream(content.get())) {
            String extractedText = tika.parseToString(stream);
            documentInputs.setExtractedText(extractedText);
        } catch (IOException | TikaException e) {
            throw new ConnectorException(String.format("Unable to extract text; URI: %s; cause: %s",
                documentInputs.getInitialUri(), e.getMessage()), e);
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

        if (textClassifier != null && chunks != null && !chunks.isEmpty()) {
            classifyChunks(inputs);
        }
    }

    private void classifyChunks(DocumentInputs inputs) {
        List<byte[]> classifications = new ArrayList<>();
        for (String chunk : inputs.getChunks()) {
            byte[] result = textClassifier.classifyTextToBytes(inputs.getInitialUri(), chunk);
            classifications.add(result);
        }
        inputs.setClassifications(classifications);
    }

    private void addEmbeddingsToSplitterChunks(DocumentInputs inputs) {
        List<float[]> embeddings = new ArrayList<>();
        for (String chunk : inputs.getChunks()) {
            float[] embedding = embeddingProducer.produceEmbedding(chunk);
            embeddings.add(embedding);
        }
        inputs.setEmbeddings(embeddings);
    }

    private void addEmbeddingsToExistingChunks(DocumentInputs inputs) {
        DocumentAndChunks documentAndChunks = chunkSelector.selectChunks(inputs.getInitialUri(), inputs.getContent());
        if (documentAndChunks != null && documentAndChunks.hasChunks()) {
            for (Chunk chunk : documentAndChunks.getChunks()) {
                String text = chunk.getEmbeddingText();
                if (text != null && text.trim().length() > 0) {
                    float[] embedding = embeddingProducer.produceEmbedding(text);
                    chunk.addEmbedding(embedding);
                }
            }
            // This part is key - the chunks above are being modified, so need to update the content in the
            // DocumentInputs object.
            inputs.setContent(documentAndChunks.getContent());
        }
    }
}
