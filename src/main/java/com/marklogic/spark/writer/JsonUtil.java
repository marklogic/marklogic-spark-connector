/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;

public interface JsonUtil {

    static JsonNode getJsonFromHandle(AbstractWriteHandle writeHandle) {
        if (writeHandle instanceof JacksonHandle) {
            return ((JacksonHandle) writeHandle).get();
        } else {
            String json = HandleAccessor.contentAsString(writeHandle);
            try {
                return new ObjectMapper().readTree(json);
            } catch (JsonProcessingException e) {
                throw new ConnectorException(String.format(
                    "Unable to read JSON from content handle; cause: %s", e.getMessage()), e);
            }
        }
    }
}
