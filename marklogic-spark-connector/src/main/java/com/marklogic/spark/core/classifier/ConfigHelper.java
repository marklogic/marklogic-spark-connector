/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

// As of 3.0.0, no longer handles the API token, as we have a handler for generating and refreshing tokens.
class ConfigHelper {

    private final String host;
    private final int port;
    private final String protocol;
    private final String classifierPath;
    private final String tokenEndpoint;
    private final Map<String, String> additionalParameters = new HashMap<>();

    ConfigHelper(Context context) {
        this.host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
        this.port = context.getIntOption(Options.WRITE_CLASSIFIER_PORT, 443, 0);
        this.protocol = "true".equalsIgnoreCase(context.getStringOption(Options.WRITE_CLASSIFIER_HTTP)) ? "http" : "https";
        this.classifierPath = fixPath(context.getStringOption(Options.WRITE_CLASSIFIER_PATH, "/"));
        this.tokenEndpoint = fixPath(context.getStringOption(Options.WRITE_CLASSIFIER_TOKEN_PATH, "/token"));

        context.getProperties().forEach((key, value) -> {
            if (key.startsWith(Options.WRITE_CLASSIFIER_OPTION_PREFIX)) {
                String name = key.substring(Options.WRITE_CLASSIFIER_OPTION_PREFIX.length());
                additionalParameters.put(name, context.getStringOption(key));
            }
        });

        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Will classify text via host {}", this.host);
        }
    }

    public ClassificationConfiguration buildClassificationConfiguration() {
        ClassificationConfiguration config = new ClassificationConfiguration();
        config.setHostName(host);
        config.setHostPort(port);
        config.setProtocol(protocol);
        config.setHostPath(classifierPath);
        if (!this.additionalParameters.isEmpty()) {
            config.setAdditionalParameters(additionalParameters);
        }
        return config;
    }

    public URL getTokenUrl() {
        try {
            return new URL(protocol, host, port, tokenEndpoint);
        } catch (MalformedURLException e) {
            throw new ConnectorException(String.format("Unable to construct token URL with endpoint: %s; cause: %s",
                tokenEndpoint, e.getMessage()), e);
        }
    }

    private String fixPath(String path) {
        if (path == null) {
            return null;
        }
        return path.startsWith("/") ? path : "/" + path;
    }
}
