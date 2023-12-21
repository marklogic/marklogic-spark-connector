/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Knows how to use a user-provided URI template for making a URI based on a Spark row.
 */
class SparkRowUriMaker implements DocBuilder.UriMaker {

    private String uriTemplate;

    // The matcher can be reused as this class is not expected to be thread-safe, as each WriteBatcherDataWriter creates
    // its own and never has multiple threads trying to access it at the same time.
    private Matcher matcher;

    SparkRowUriMaker(String uriTemplate) {
        validateUriTemplate(uriTemplate);
        this.uriTemplate = uriTemplate;
        this.matcher = Pattern.compile("\\{([^}]+)\\}", Pattern.CASE_INSENSITIVE).matcher(uriTemplate);
    }

    @Override
    public String makeURI(String initialUri, ObjectNode columnValues) {
        // initialUri is ignored as the intent is to build the entire URI from the template.
        // Inspired by https://www.baeldung.com/java-regex-token-replacement
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        while (matcher.find()) {
            output.append(this.uriTemplate, lastIndex, matcher.start());
            String columnName = matcher.group(1);
            output.append(getColumnValue(columnValues, columnName));
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
        final String preamble = String.format("Invalid value for %s: %s; ", Options.WRITE_URI_TEMPLATE, uriTemplate);
        boolean inToken = false;
        int tokenSize = 0;
        char[] chars = uriTemplate.toCharArray();
        for (char ch : chars) {
            if (ch == '}') {
                if (!inToken) {
                    throw new IllegalArgumentException(preamble + "closing brace found before opening brace");
                }
                if (tokenSize == 0) {
                    throw new IllegalArgumentException(preamble + "no column name within opening and closing brace");
                }
                inToken = false;
            } else if (ch == '{') {
                if (inToken) {
                    throw new IllegalArgumentException(preamble + "expected closing brace, but found opening brace");
                }
                inToken = true;
                tokenSize = 0;
            } else if (inToken) {
                tokenSize++;
            }
        }
        if (inToken) {
            throw new IllegalArgumentException(preamble + "opening brace without closing brace");
        }
    }

    private String getColumnValue(JsonNode row, String columnName) {
        if (!row.has(columnName)) {
            throw new ConnectorException(
                String.format("Did not find column '%s' in row: %s; column is required by URI template: %s",
                    columnName, row, uriTemplate
                ));
        }

        String text = row.get(columnName).asText();
        if (text.trim().length() == 0) {
            throw new ConnectorException(
                String.format("Column '%s' is empty in row: %s; column is required by URI template: %s",
                    columnName, row, uriTemplate
                ));
        }

        return text;
    }

}
