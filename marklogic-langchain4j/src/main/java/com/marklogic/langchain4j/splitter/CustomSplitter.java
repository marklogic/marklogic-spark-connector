/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;

import java.util.List;
import java.util.Map;

/**
 * This is a silly implementation, but it is sufficient for ensuring a custom splitter can be invoked with custom
 * options passed to it.
 */
public class CustomSplitter implements DocumentSplitter {

    private String textToReturn;

    public CustomSplitter(Map<String, String> options) {
        this.textToReturn = options.get("textToReturn");
        if (this.textToReturn == null) {
            this.textToReturn = "You passed in null!";
        }
    }

    @Override
    public List<TextSegment> split(Document document) {
        return List.of(new TextSegment(textToReturn, new Metadata()));
    }
}
