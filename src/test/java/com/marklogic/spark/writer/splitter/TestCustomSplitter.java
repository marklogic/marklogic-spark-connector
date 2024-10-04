/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestCustomSplitter implements DocumentSplitter {

    private final Map<String, String> options;

    public TestCustomSplitter(Map<String, String> options) {
        this.options = options;
    }

    @Override
    public List<TextSegment> split(Document document) {
        String preamble = options.get("preamble");
        return Stream.of("hello", "world")
            .map(value -> new TextSegment(preamble + value, new Metadata()))
            .collect(Collectors.toList());
    }
}
