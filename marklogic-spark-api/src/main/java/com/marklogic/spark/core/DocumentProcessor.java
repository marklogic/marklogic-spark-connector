/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.core.embedding.ChunkSelector;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Handles "processing" a document, which involves receiving a {@code DocumentInputs} instance, enriching it,
 * and returning one or more input instances.
 */
public class DocumentProcessor {

    private final Tika tika;
    private final TextSplitter textSplitter;
    private final TextClassifier textClassifier;
    private final EmbeddingProducer embeddingProducer;
    private final ChunkSelector chunkSelector;

    public DocumentProcessor(Tika tika, TextSplitter textSplitter, TextClassifier textClassifier,
                             EmbeddingProducer embeddingProducer, ChunkSelector chunkSelector) {
        this.tika = tika;
        this.textSplitter = textSplitter;
        this.textClassifier = textClassifier;
        this.embeddingProducer = embeddingProducer;
        this.chunkSelector = chunkSelector;
    }

    public Optional<List<DocumentInputs>> flush() {
        if (embeddingProducer != null) {
            return Optional.of(embeddingProducer.flush());
        }
        return Optional.empty();
    }

    public List<DocumentInputs> processDocument(DocumentInputs inputs) {
        if (tika != null && inputs.getContent() instanceof BytesHandle) {
            extractText(inputs);
        }
        if (textSplitter != null) {
            applySplitter(inputs);
        }
        if (textClassifier != null) {
            classifyText(inputs);
        }

        if (embeddingProducer != null) {
            if (inputs.getChunks() != null && !inputs.getChunks().isEmpty()) {
                return embeddingProducer.produceEmbeddings(inputs);
            } else if (chunkSelector != null) {
                DocumentAndChunks documentAndChunks = chunkSelector.selectChunks(inputs.getInitialUri(), inputs.getContent());
                if (documentAndChunks != null && documentAndChunks.hasChunks()) {
                    inputs.setContentAndExistingChunks(documentAndChunks);
                    return embeddingProducer.produceEmbeddings(inputs);
                }
            }
        }

        return Arrays.asList(inputs);
    }

    private void extractText(DocumentInputs inputs) {
        BytesHandle content = (BytesHandle) inputs.getContent();
        try (ByteArrayInputStream stream = new ByteArrayInputStream(content.get())) {
            String extractedText = tika.parseToString(stream);
            inputs.setExtractedText(extractedText);
        } catch (IOException | TikaException e) {
            throw new ConnectorException(String.format("Unable to extract text; URI: %s; cause: %s",
                inputs.getInitialUri(), e.getMessage()), e);
        }
    }

    private void classifyText(DocumentInputs inputs) {
        // Not sure if we should run the classifier if the user specifies a splitter. We may need options to control
        // whether this happens or not.
        final String uri = inputs.getInitialUri();
        if (inputs.getExtractedText() != null) {
            byte[] classification = textClassifier.classifyTextToBytes(uri, inputs.getExtractedText());
            inputs.setClassificationResponse(classification);
        } else {
            String content = HandleAccessor.contentAsString(inputs.getContent());
            byte[] classification = textClassifier.classifyTextToBytes(uri, content);
            inputs.setClassificationResponse(classification);
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

    // Should this impact existing chunks???
    private void classifyChunks(DocumentInputs inputs) {
        List<byte[]> classifications = new ArrayList<>();
        for (String chunk : inputs.getChunks()) {
            byte[] result = textClassifier.classifyTextToBytes(inputs.getInitialUri(), chunk);
            classifications.add(result);
        }
        inputs.setClassifications(classifications);
    }
}
