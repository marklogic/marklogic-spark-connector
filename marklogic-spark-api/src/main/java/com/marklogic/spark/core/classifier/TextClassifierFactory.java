
/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.TokenFetcher;

import java.net.MalformedURLException;
import java.net.URL;


public interface TextClassifierFactory {

    static TextClassifier newTextClassifier(Context context) {
        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
        if (host != null && host.trim().length() > 0) {
            try {
                ClassificationConfiguration config = buildClassificationConfiguration(context);
                return new TextClassifier(config);
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to create a TextClassifier; cause: %s", e.getMessage()));
            }
        } else {
            return null;
        }
    }

    private static ClassificationConfiguration buildClassificationConfiguration(Context context) throws MalformedURLException, CloudException {
        final ConfigHelper configHelper = new ConfigHelper(context);
        final String apiKey = context.getStringOption(Options.WRITE_CLASSIFIER_APIKEY);
        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Will classify text via host {}; token URL: {}",
                configHelper.getHost(), configHelper.getTokenUrl());
        }

        TokenFetcher tokenFetcher = new TokenFetcher(configHelper.getTokenUrl().toString(), apiKey);

        ClassificationConfiguration config = new ClassificationConfiguration();
        config.setApiToken(tokenFetcher.getAccessToken().getAccess_token());

        config.setHostName(configHelper.getHost());
        config.setHostPort(configHelper.getPort());
        config.setProtocol(configHelper.getProtocol());
        config.setHostPath(configHelper.getClassifierPath());

        return config;
    }

    class ConfigHelper {
        private final String host;
        private final int port;
        private final String protocol;
        private final String classifierPath;
        private final URL tokenUrl;

        public ConfigHelper(Context context) throws MalformedURLException {
            this.host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
            this.port = context.getIntOption(Options.WRITE_CLASSIFIER_PORT, 443, 0);
            this.protocol = "true".equalsIgnoreCase(context.getStringOption(Options.WRITE_CLASSIFIER_HTTP)) ? "http" : "https";
            this.classifierPath = fixPath(context.getStringOption(Options.WRITE_CLASSIFIER_PATH));
            String tokenEndpoint = fixPath(context.getStringOption(Options.WRITE_CLASSIFIER_TOKEN_PATH, "/token"));
            this.tokenUrl = new URL(protocol, host, port, tokenEndpoint);
        }

        public URL getTokenUrl() {
            return this.tokenUrl;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getProtocol() {
            return protocol;
        }

        public String getClassifierPath() {
            return classifierPath;
        }

        private String fixPath(String path) {
            if (path == null) {
                return null;
            }
            return path.startsWith("/") ? path : "/" + path;
        }
    }
}
