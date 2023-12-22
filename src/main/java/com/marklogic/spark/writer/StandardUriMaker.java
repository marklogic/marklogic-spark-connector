package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.ConnectorException;

import java.util.UUID;

class StandardUriMaker implements DocBuilder.UriMaker {

    private String uriPrefix;
    private String uriSuffix;
    private String uriReplace;

    StandardUriMaker(String uriPrefix, String uriSuffix, String uriReplace) {
        this.uriPrefix = uriPrefix;
        this.uriSuffix = uriSuffix;
        this.uriReplace = uriReplace;
    }

    @Override
    public String makeURI(String initialUri, ObjectNode columnValues) {
        String uri = initialUri != null ? initialUri : "";
        if (uriReplace != null && uriReplace.trim().length() > 0) {
            uri = applyUriReplace(uri);
        }
        if (uriPrefix != null) {
            uri = uriPrefix + uri;
        }
        if (initialUri == null || initialUri.trim().length() == 0) {
            uri += UUID.randomUUID().toString();
        }
        return uriSuffix != null ? uri + uriSuffix : uri;
    }

    private String applyUriReplace(String uri) {
        String[] expressions = uriReplace.split(",");
        if (expressions.length % 2 != 0) {
            throw new ConnectorException(String.format(
                "The URI replacement expression must contain an equal number of patterns and replacement strings: %s", uriReplace));
        }
        for (int i = 0; i < expressions.length; i += 2) {
            String regex = expressions[i];
            String replacement = expressions[i + 1];
            if (!replacement.startsWith("'") || !replacement.endsWith("'")) {
                throw new ConnectorException(String.format(
                    "Each URI replacement value must be surrounded with single quotes: %s", uriReplace
                ));
            }
            replacement = replacement.substring(1, replacement.length() - 1);
            uri = uri.replaceAll(regex, replacement);
        }
        return uri;
    }
}
