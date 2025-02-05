/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.langchain4j.splitter.AllTextSelector;
import com.marklogic.langchain4j.splitter.TextSelector;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.langchain4j.DocumentSplitterFactory;
import com.marklogic.spark.langchain4j.DocumentTextSplitterFactory;
import dev.langchain4j.data.document.DocumentSplitter;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TextSplitterConfig implements Serializable {

    private int maxChunkSize = 1000;
    private int maxOverlapSize;
    private String xpathExpression;
    private List<String> jsonPointers;
    private String regex;
    private String joinDelimiter;
    private String customClass;
    private Map<String, String> customClassOptions;
    private Map<String, String> namespaces;

    // Sonar is reporting functions.udf as deprecated, but it doesn't appear to be. So ignoring Sonar warning.
    @SuppressWarnings("java:S1874")
    public UserDefinedFunction buildUDF() {
        return functions.udf(
            new TextSplitter(this),
            DataTypes.createArrayType(DataTypes.StringType)
        );
    }

    TextSelector buildTextSelector() {
        if (this.jsonPointers != null) {
            String newlineDelimitedPointers = this.jsonPointers.stream().collect(Collectors.joining("\n"));
            return DocumentTextSplitterFactory.makeJsonTextSelector(newlineDelimitedPointers);
        } else if (this.xpathExpression != null) {
            Map<String, String> properties = new HashMap<>();
            if (namespaces != null) {
                namespaces.entrySet().forEach(entry ->
                    properties.put(Options.XPATH_NAMESPACE_PREFIX + entry.getKey(), entry.getValue()));
            }
            return DocumentTextSplitterFactory.makeXmlTextSelector(new Context(properties));
        } else {
            return new AllTextSelector();
        }
    }

    DocumentSplitter buildDocumentSplitter() {
        Map<String, String> properties = new HashMap<>();
        properties.put(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, Integer.toString(maxChunkSize));
        properties.put(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, Integer.toString(maxOverlapSize));
        properties.put(Options.WRITE_SPLITTER_REGEX, regex);
        properties.put(Options.WRITE_SPLITTER_JOIN_DELIMITER, joinDelimiter);
        properties.put(Options.WRITE_SPLITTER_CUSTOM_CLASS, customClass);
        if (customClassOptions != null) {
            customClassOptions.entrySet().forEach(entry ->
                properties.put(Options.WRITE_SPLITTER_CUSTOM_CLASS_OPTION_PREFIX + entry.getKey(), entry.getValue()));
        }
        return DocumentSplitterFactory.makeDocumentSplitter(new Context(properties));
    }

    public void setMaxChunkSize(int maxChunkSize) {
        this.maxChunkSize = maxChunkSize;
    }

    public void setMaxOverlapSize(int maxOverlapSize) {
        this.maxOverlapSize = maxOverlapSize;
    }

    public void setXpathExpression(String xpathExpression) {
        this.xpathExpression = xpathExpression;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public void setJoinDelimiter(String joinDelimiter) {
        this.joinDelimiter = joinDelimiter;
    }

    public void setCustomClass(String customClass) {
        this.customClass = customClass;
    }

    public void setCustomClassOptions(Map<String, String> customClassOptions) {
        this.customClassOptions = customClassOptions;
    }

    public void setNamespaces(Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    public void setJsonPointers(List<String> jsonPointers) {
        this.jsonPointers = jsonPointers;
    }
}
