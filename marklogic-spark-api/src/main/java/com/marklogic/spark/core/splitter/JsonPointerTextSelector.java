/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JsonPointerTextSelector implements TextSelector {

    private final List<JsonPointer> jsonPointers;
    private final String joinDelimiter;

    public JsonPointerTextSelector(String[] jsonPointerArray, String joinDelimiter) {
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
    public String selectTextToSplit(DocumentWriteOperation sourceDocument) {
        return selectTextToSplit(sourceDocument.getContent(), sourceDocument.getUri());

    }

    @Override
    public String selectTextToSplit(AbstractWriteHandle contentHandle) {
        return selectTextToSplit(contentHandle, null);
    }

    private String selectTextToSplit(AbstractWriteHandle contentHandle, String uriForLogMessage) {
        JsonNode doc;
        try {
            doc = com.marklogic.spark.Util.getJsonFromHandle(contentHandle);
        } catch (Exception ex) {
            Util.MAIN_LOGGER.warn("Unable to select text to split in document: {}; cause: {}", uriForLogMessage, ex.getMessage());
            return null;
        }

        return jsonPointers.stream()
            .map(jsonPointer -> {
                JsonNode result = doc.at(jsonPointer);
                return result.isValueNode() ? result.asText() : result.toString();
            })
            .collect(Collectors.joining(joinDelimiter));
    }
}
