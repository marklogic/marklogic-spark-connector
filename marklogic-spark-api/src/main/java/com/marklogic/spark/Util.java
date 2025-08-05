/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.*;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public interface Util {

    /**
     * Intended for all non-debug logging where the class name doesn't matter and only adds complexity to the log
     * messages.
     */
    Logger MAIN_LOGGER = LoggerFactory.getLogger("com.marklogic.spark");

    String DEFAULT_XML_NAMESPACE = "http://marklogic.com/appservices/model";

    static boolean hasOption(Map<String, String> properties, String... options) {
        return Stream.of(options)
            .anyMatch(option -> properties.get(option) != null && !properties.get(option).trim().isEmpty());
    }

    /**
     * For parsing the Spark "paths" option, which is set when a user calls {@code load()} with 2 or more
     * paths. In that scenario, Spark effectively toString's the list of paths into a string of the form:
     * "["path", "path2", "path3"]", with the surrounding double quotes included.
     *
     * @param pathsValue
     * @return
     */
    static List<String> parsePaths(String pathsValue) {
        List<String> paths = new ArrayList<>();
        pathsValue = pathsValue.trim().substring(2, pathsValue.length() - 2);
        for (String path : pathsValue.split(",")) {
            if (path.charAt(0) == '"') {
                path = path.substring(1);
            }
            if (path.charAt(path.length() - 1) == '"') {
                path = path.substring(0, path.length() - 1);
            }
            paths.add(path);
        }
        return paths;
    }

    static boolean isReadWithCustomCodeOperation(Map<String, String> properties) {
        return Util.hasOption(properties,
            Options.READ_INVOKE, Options.READ_XQUERY, Options.READ_JAVASCRIPT,
            Options.READ_JAVASCRIPT_FILE, Options.READ_XQUERY_FILE
        );
    }

    static boolean isWriteWithCustomCodeOperation(Map<String, String> properties) {
        return Util.hasOption(properties,
            Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY,
            Options.WRITE_JAVASCRIPT_FILE, Options.WRITE_XQUERY_FILE
        );
    }

    /**
     * Allows Flux to override what's shown in a validation error. The connector is fine showing option names
     * such as "spark.marklogic.read.opticQuery", but that is meaningless to a Flux user. This can also be used to
     * access any key in the messages properties file.
     *
     * @param option
     * @return
     */
    static String getOptionNameForErrorMessage(String option) {
        ResourceBundle bundle = ResourceBundle.getBundle("marklogic-spark-messages", Locale.getDefault());
        String optionName = bundle.getString(option);
        return optionName != null && !optionName.trim().isEmpty() ? optionName.trim() : option;
    }

    static Format determineSourceDocumentFormat(AbstractWriteHandle content, String sourceUri) {
        final String uri = sourceUri != null ? sourceUri : "";
        if (content instanceof JacksonHandle || uri.endsWith(".json")) {
            return Format.JSON;
        }
        if (content instanceof DOMHandle || content instanceof JDOMHandle || uri.endsWith(".xml")) {
            return Format.XML;
        }
        if (content instanceof BaseHandle) {
            return ((BaseHandle) content).getFormat();
        }
        return null;
    }

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
                throw new ConnectorException(message);
            }
            throw ex;
        }
    }
}
