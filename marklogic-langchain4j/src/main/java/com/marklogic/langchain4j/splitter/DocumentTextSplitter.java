/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.core.splitter.TextSelector;
import com.marklogic.spark.core.splitter.TextSplitter;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the "splitter" capability by delegating to different objects for selecting text to split; splitting
 * the selected text; and then processing the given chunks
 */
public class DocumentTextSplitter implements TextSplitter {

    private final TextSelector textSelector;
    private final DocumentSplitter documentSplitter;

    public DocumentTextSplitter(TextSelector textSelector, DocumentSplitter documentSplitter) {
        this.textSelector = textSelector;
        this.documentSplitter = documentSplitter;
    }

    @Override
    public List<String> split(String sourceUri, AbstractWriteHandle content) {
        String text = textSelector.selectTextToSplit(content);
        if (text == null || text.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<TextSegment> textSegments;
        try {
            textSegments = documentSplitter.split(new Document(text));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to split document with URI: %s; cause: %s",
                sourceUri, e.getMessage()), e);
        }

        return textSegments.stream().map(TextSegment::text).toList();
    }
}
