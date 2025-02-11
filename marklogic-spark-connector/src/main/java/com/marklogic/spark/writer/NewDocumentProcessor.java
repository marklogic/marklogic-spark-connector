/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.classifier.TextClassifier;
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

    public NewDocumentProcessor(Tika tika, TextSplitter textSplitter, TextClassifier textClassifier) {
        this.tika = tika;
        this.textSplitter = textSplitter;
        this.textClassifier = textClassifier;
    }

    public void processDocument(DocBuilder.DocumentInputs inputs) {
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
    }

    private void extractText(DocBuilder.DocumentInputs documentInputs) {
        BytesHandle content = (BytesHandle) documentInputs.getContent();
        try (ByteArrayInputStream stream = new ByteArrayInputStream(content.get())) {
            String extractedText = tika.parseToString(stream);
            documentInputs.setExtractedText(extractedText);
        } catch (IOException | TikaException e) {
            throw new ConnectorException(String.format("Unable to extract text; URI: %s; cause: %s",
                documentInputs.getInitialUri(), e.getMessage()), e);
        }
    }

    private void applySplitter(DocBuilder.DocumentInputs inputs) {
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

    private void classifyChunks(DocBuilder.DocumentInputs inputs) {
        List<byte[]> classifications = new ArrayList<>();
        for (String chunk : inputs.getChunks()) {
            byte[] result = textClassifier.classifyTextToBytes(inputs.getInitialUri(), chunk);
            classifications.add(result);
        }
        inputs.setClassifications(classifications);
    }
}
