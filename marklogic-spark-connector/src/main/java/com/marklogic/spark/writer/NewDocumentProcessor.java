/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * This will soon handle extracting, splitting, classifying, and embedding.
 * And ideally, it and DocBuilder can all be moved to the marklogic-spark-api project where
 * it can be more easily reused in the future.
 */
public class NewDocumentProcessor {

    private final Tika tika;
    private final TextSplitter textSplitter;

    public NewDocumentProcessor(Tika tika, TextSplitter textSplitter) {
        this.tika = tika;
        this.textSplitter = textSplitter;
    }

    public DocBuilder.DocumentInputs processDocument(DocBuilder.DocumentInputs inputs) {
        if (tika != null && inputs.getContent() instanceof BytesHandle) {
            extractText(inputs);
        }
        if (textSplitter != null) {
            if (inputs.getExtractedText() != null) {
                StringHandle content = new StringHandle(inputs.getExtractedText());
                inputs.setChunks(textSplitter.split(inputs.getInitialUri(), content));
            } else {
                inputs.setChunks(textSplitter.split(inputs.getInitialUri(), inputs.getContent()));
            }
        }
        return inputs;
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
}
