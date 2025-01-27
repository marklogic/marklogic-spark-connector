/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.client.io.BytesHandle;
import com.marklogic.langchain4j.splitter.TextSelector;
import com.marklogic.spark.langchain4j.DocumentTextSplitterFactory;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.segment.TextSegment;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TextSplitter implements UDF1<Object, List<String>> {

    private int maxChunkSize;
    private int maxOverlapSize;
    private String jsonPointer;

    public static UserDefinedFunction build() {
        return build(1000, 0, null);
    }

    // Initial attempt at configuration. Will shifter to a real Builder class soon.
    public static UserDefinedFunction build(int maxChunkSize, int maxOverlapSize, String jsonPointer) {
        TextSplitter splitter = new TextSplitter();
        splitter.maxChunkSize = maxChunkSize;
        splitter.maxOverlapSize = maxOverlapSize;
        splitter.jsonPointer = jsonPointer;
        return functions.udf(splitter, DataTypes.createArrayType(DataTypes.StringType));
    }

    @Override
    public List<String> call(Object columnValue) throws Exception {
        if (columnValue == null) {
            return new ArrayList<>();
        }

        DocumentSplitter splitter = DocumentSplitters.recursive(maxChunkSize, maxOverlapSize);

        if (columnValue instanceof String) {
            return splitter.split(new Document((String) columnValue))
                .stream().map(TextSegment::text)
                .collect(Collectors.toList());
        } else if (columnValue instanceof byte[]) {
            TextSelector textSelector = DocumentTextSplitterFactory.makeJsonTextSelector(this.jsonPointer);
            String text = textSelector.selectTextToSplit(new BytesHandle((byte[]) columnValue));
            return splitter.split(new Document(text))
                .stream().map(TextSegment::text)
                .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("Unable to split text from column value: " + columnValue);
        }
    }
}
