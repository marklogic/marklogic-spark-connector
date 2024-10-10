/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.writer.JsonUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JSONPointerTextSelector implements TextSelector {

    private final List<JsonPointer> jsonPointers;
    private final String joinDelimiter;

    public JSONPointerTextSelector(String[] jsonPointerArray, String joinDelimiter) {
        jsonPointers = new ArrayList<>();
        for (String jsonPointer : jsonPointerArray) {
            try {
                jsonPointers.add(JsonPointer.compile(jsonPointer));
            } catch (Exception ex) {
                // Not including the original exception as the message itself should suffice.
                throw new ConnectorException(String.format(
                    "Unable to use JSON pointer expression: %s; cause: %s", jsonPointer, ex.getMessage()));
            }
        }
        this.joinDelimiter = joinDelimiter != null ? joinDelimiter : " ";
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation operation) {
        JsonNode doc = JsonUtil.getJsonFromHandle(operation.getContent());
        return jsonPointers.stream()
            .map(jsonPointer -> doc.at(jsonPointer).asText())
            .collect(Collectors.joining(joinDelimiter));
    }
}
