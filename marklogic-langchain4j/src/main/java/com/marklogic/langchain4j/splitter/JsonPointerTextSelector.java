/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.langchain4j.MarkLogicLangchainException;
import com.marklogic.langchain4j.Util;

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
                throw new MarkLogicLangchainException(String.format(
                    "Unable to use JSON pointer expression: %s; cause: %s", jsonPointer, ex.getMessage()));
            }
        }
        this.joinDelimiter = joinDelimiter != null ? joinDelimiter : " ";
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation sourceDocument) {
        JsonNode doc;
        try {
            doc = Util.getJsonFromHandle(sourceDocument.getContent());
        } catch (Exception ex) {
            Util.LANGCHAIN4J_LOGGER.warn("Unable to select text to split in document: {}; cause: {}", sourceDocument.getUri(), ex.getMessage());
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
