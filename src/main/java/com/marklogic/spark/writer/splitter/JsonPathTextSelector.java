/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.HandleAccessor;

public class JsonPathTextSelector implements TextSelector {

    private String path;

    public JsonPathTextSelector(String path) {
        this.path = path;
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation operation) {
        String json = HandleAccessor.contentAsString(operation.getContent());
        Object node = Configuration.defaultConfiguration().jsonProvider().parse(json);
        return JsonPath.read(node, path);
    }
}
