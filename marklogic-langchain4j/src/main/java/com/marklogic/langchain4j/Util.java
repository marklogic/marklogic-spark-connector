/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Util {

    /**
     * Intended for log messages pertaining to the embedder feature. Uses a separate logger so that it can be enabled
     * at the info/debug level without enabling any other log messages.
     */
    Logger LANGCHAIN4J_LOGGER = LoggerFactory.getLogger("com.marklogic.langchain4j");

    String DEFAULT_XML_NAMESPACE = "http://marklogic.com/appservices/model";

    static JsonNode getJsonFromHandle(AbstractWriteHandle writeHandle) {
        if (writeHandle instanceof JacksonHandle) {
            return ((JacksonHandle) writeHandle).get();
        } else {
            String json = HandleAccessor.contentAsString(writeHandle);
            try {
                return new ObjectMapper().readTree(json);
            } catch (JsonProcessingException e) {
                throw new MarkLogicLangchainException(String.format(
                    "Unable to read JSON from content handle; cause: %s", e.getMessage()), e);
            }
        }
    }

    static void addPermissionsFromDelimitedString(DocumentMetadataHandle.DocumentPermissions permissions,
                                                  String rolesAndCapabilities) {
        // This isn't likely the best home for this class, but it's needed by this module and by the connector to
        // massage an error message that is meaningful for a Java Client user but not meaningful for a connector
        // or Flux user.
        try {
            permissions.addFromDelimitedString(rolesAndCapabilities);
        } catch (IllegalArgumentException ex) {
            String message = ex.getMessage();
            final String confusingMessageForUser = "No enum constant com.marklogic.client.io.DocumentMetadataHandle.Capability.";
            if (message != null && message.contains(confusingMessageForUser)) {
                message = message.replace(confusingMessageForUser, "Not a valid capability: ");
                throw new IllegalArgumentException(message, ex);
            }
            throw ex;
        }
    }

}
