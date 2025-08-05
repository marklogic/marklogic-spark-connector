/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.ConnectorException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Knows how to use a user-provided URI template for making a URI based on a Spark row.
 */
class SparkRowUriMaker implements DocBuilder.UriMaker {

    private final String uriTemplate;
    private final String uriTemplateOptionName;

    // The matcher can be reused as this class is not expected to be thread-safe, as each WriteBatcherDataWriter creates
    // its own and never has multiple threads trying to access it at the same time.
    private Matcher matcher;

    SparkRowUriMaker(String uriTemplate, String uriTemplateOptionName) {
        this.uriTemplate = uriTemplate;
        this.uriTemplateOptionName = uriTemplateOptionName;
        validateUriTemplate(uriTemplate);
        this.matcher = Pattern.compile("\\{([^}]+)\\}", Pattern.CASE_INSENSITIVE).matcher(uriTemplate);
    }

    @Override
    public String makeURI(String initialUri, JsonNode uriTemplateValues) {
        if (uriTemplateValues == null) {
            throw new ConnectorException(String.format("Unable to create URI using template '%s' for initial URI '%s'; no URI template values found.",
                this.uriTemplate, initialUri));
        }

        // initialUri is ignored as the intent is to build the entire URI from the template.
        // Inspired by https://www.baeldung.com/java-regex-token-replacement
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        while (matcher.find()) {
            output.append(this.uriTemplate, lastIndex, matcher.start());
            String expression = matcher.group(1);
            output.append(getExpressionValue(uriTemplateValues, expression));
            lastIndex = matcher.end();
        }
        if (lastIndex < this.uriTemplate.length()) {
            output.append(this.uriTemplate, lastIndex, this.uriTemplate.length());
        }
        matcher.reset();

        return output.toString();
    }

    private void validateUriTemplate(String uriTemplate) {
        // Copied from the DHF Spark 2 connector
        final String preamble = String.format("Invalid value for %s: %s; ", uriTemplateOptionName, uriTemplate);
        boolean inToken = false;
        int tokenSize = 0;
        char[] chars = uriTemplate.toCharArray();
        for (char ch : chars) {
            if (ch == '}') {
                if (!inToken) {
                    throw new ConnectorException(preamble + "closing brace found before opening brace");
                }
                if (tokenSize == 0) {
                    throw new ConnectorException(preamble + "no column name within opening and closing brace");
                }
                inToken = false;
            } else if (ch == '{') {
                if (inToken) {
                    throw new ConnectorException(preamble + "expected closing brace, but found opening brace");
                }
                inToken = true;
                tokenSize = 0;
            } else if (inToken) {
                tokenSize++;
            }
        }
        if (inToken) {
            throw new ConnectorException(preamble + "opening brace without closing brace");
        }
    }

    private String getExpressionValue(JsonNode uriTemplateValues, String expression) {
        JsonNode node;
        // As of 2.3.0, now supports a JSONPointer expression, which is indicated by the first character being a "/".
        if (expression.startsWith("/")) {
            node = uriTemplateValues.at(expression);
        } else {
            node = uriTemplateValues.has(expression) ? uriTemplateValues.get(expression) : null;
        }

        if (node == null || node.isMissingNode()) {
            throw new ConnectorException(
                String.format("Expression '%s' did not resolve to a value in row: %s; expression is required by URI template: %s",
                    expression, uriTemplateValues, uriTemplate
                ));
        }

        String text = node.asText();
        if (text.trim().isEmpty()) {
            throw new ConnectorException(
                String.format("Expression '%s' resolved to an empty string in row: %s; expression is required by URI template: %s",
                    expression, uriTemplateValues, uriTemplate
                ));
        }
        return text;
    }
}
