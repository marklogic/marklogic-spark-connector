
/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.TokenFetcher;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public abstract class TextClassifierFactory {

    private static final String MOCK_CLASSIFIER_OPTION = "spark.marklogic.testing.mockClassifierResponse";

    public static TextClassifier newTextClassifier(Context context) {
        if (context.hasOption(MOCK_CLASSIFIER_OPTION)) {
            return new MockTextClassifier(context.getStringOption(MOCK_CLASSIFIER_OPTION));
        }

        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
        if (host != null && host.trim().length() > 0) {
            try {
                ClassificationConfiguration config = buildClassificationConfiguration(context);
                return new SemaphoreTextClassifier(config);
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
        TokenFetcher tokenFetcher = new TokenFetcher(configHelper.getTokenUrl().toString(), apiKey);
        String apiToken = tokenFetcher.getAccessToken().getAccess_token();
        return configHelper.buildClassificationConfiguration(apiToken);
    }

    public static class ConfigHelper {
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

            if (SemaphoreTextClassifier.CLASSIFIER_LOGGER.isInfoEnabled()) {
                SemaphoreTextClassifier.CLASSIFIER_LOGGER
                    .info("Will classify text via host {}; token URL: {}", this.host, this.tokenUrl);
            }
        }

        public ClassificationConfiguration buildClassificationConfiguration(String apiToken) {
            ClassificationConfiguration config = new ClassificationConfiguration();
            config.setApiToken(apiToken);

            config.setHostName(host);
            config.setHostPort(port);
            config.setProtocol(protocol);
            config.setHostPath(classifierPath);

            Map<String, String> additionalParameters = new HashMap<>();
            additionalParameters.put("threshold", "20");
            additionalParameters.put("language", "en1");
            config.setAdditionalParameters(additionalParameters);
            return config;
        }

        public URL getTokenUrl() {
            return this.tokenUrl;
        }

        private String fixPath(String path) {
            if (path == null) {
                return null;
            }
            return path.startsWith("/") ? path : "/" + path;
        }
    }

    private TextClassifierFactory() {
    }

    public static class MockTextClassifier implements TextClassifier {

        private final String mockResponse;
        private static boolean wasClosed;

        private MockTextClassifier(String mockResponse) {
            this.mockResponse = mockResponse;
        }

        @Override
        public byte[] classifyText(String sourceUri, String text) {
            return mockResponse.getBytes();
        }

        public static boolean isClosed() {
            return wasClosed;
        }

        @Override
        // Sonar doesn't like this, but it's fine in a class that's only used as a mock.
        @SuppressWarnings("java:S2696")
        public void close() {
            MockTextClassifier.wasClosed = true;
        }
    }
}
