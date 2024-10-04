/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.writer.JsonUtil;

/**
 * TODO May want a delimiter?
 */
public class JsonTextSelector implements TextSelector {

    private JsonPointer jsonPointer;

    public JsonTextSelector(String jsonPointerExpression) {
        this.jsonPointer = JsonPointer.compile(jsonPointerExpression);
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation operation) {
        JsonNode doc = JsonUtil.getJsonFromHandle(operation.getContent());
        // This could take an option of - don't get the text, get the node and toString it.
        System.out.println(doc.toPrettyString());
        return doc.at(this.jsonPointer).asText();
    }
}
