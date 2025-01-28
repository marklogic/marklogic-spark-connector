/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.langchain4j.splitter.TextSelector;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TextSplitter implements UDF1<Object, List<String>>, Serializable {

    private final TextSplitterConfig textSplitterConfig;

    // A UDF must be serializable, and these are not. So they're lazily created based on the config.
    private transient TextSelector textSelector;
    private transient DocumentSplitter documentSplitter;

    public TextSplitter(TextSplitterConfig textSplitterConfig) {
        this.textSplitterConfig = textSplitterConfig;
    }

    @Override
    public List<String> call(Object columnValue) {
        if (columnValue == null) {
            return new ArrayList<>();
        }
        if (documentSplitter == null) {
            this.documentSplitter = textSplitterConfig.buildDocumentSplitter();
        }

        if (columnValue instanceof String) {
            return splitExtractedText((String) columnValue);
        } else if (columnValue instanceof byte[]) {
            return splitDocumentContent((byte[]) columnValue);
        } else {
            throw new IllegalArgumentException("Unable to split text from column value: " + columnValue);
        }
    }

    private List<String> splitExtractedText(String columnValue) {
        return documentSplitter.split(new Document(columnValue))
            .stream().map(TextSegment::text)
            .collect(Collectors.toList());
    }

    private List<String> splitDocumentContent(byte[] columnValue) {
        if (textSelector == null) {
            this.textSelector = textSplitterConfig.buildTextSelector();
        }
        String text = textSelector.selectTextToSplit(new BytesHandle(columnValue));
        return documentSplitter.split(new Document(text))
            .stream().map(TextSegment::text)
            .collect(Collectors.toList());
    }
}
