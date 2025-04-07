
/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.dom.DOMHelper;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.TokenFetcher;
import org.w3c.dom.Document;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public abstract class TextClassifierFactory {

    private static final String MOCK_CLASSIFIER_OPTION = "spark.marklogic.testing.mockClassifierResponse";

    public static TextClassifier newTextClassifier(Context context) {
        MultiArticleClassifier multiArticleClassifier = null;
        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);

        if (context.hasOption(MOCK_CLASSIFIER_OPTION)) {
            multiArticleClassifier = new MockTextClassifier(context.getStringOption(MOCK_CLASSIFIER_OPTION));
        } else if (host != null && !host.trim().isEmpty()) {
            try {
                ClassificationConfiguration config = buildClassificationConfiguration(context);
                multiArticleClassifier = new SemaphoreMultiArticleClassifier(config);
            } catch (ConnectorException ex) {
                throw ex;
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to configure a connection for classifying text; cause: %s",
                    e.getMessage()), e);
            }
        }

        if (multiArticleClassifier != null) {
            // We may need a dedicated encoding for this
            String encoding = context.getStringOption(Options.READ_FILES_ENCODING, "UTF-8");
            int batchSize = context.getIntOption(Options.WRITE_CLASSIFIER_BATCH_SIZE, 20, 1);
            return new SemaphoreTextClassifier(multiArticleClassifier, encoding, batchSize);
        }
        return null;
    }

    protected static ClassificationConfiguration buildClassificationConfiguration(Context context) {
        final ConfigHelper configHelper = new ConfigHelper(context);
        final String apiKey = context.getStringOption(Options.WRITE_CLASSIFIER_APIKEY);
        String apiToken = null;
        if (apiKey != null && !apiKey.trim().isEmpty()) {
            apiToken = generateApiToken(configHelper, apiKey);
        }
        return configHelper.buildClassificationConfiguration(apiToken);
    }

    private static String generateApiToken(ConfigHelper configHelper, String apiKey) {
        TokenFetcher tokenFetcher = new TokenFetcher(configHelper.getTokenUrl().toString(), apiKey);
        try {
            return tokenFetcher.getAccessToken().getAccess_token();
        } catch (CloudException e) {
            throw new ConnectorException(String.format("Unable to generate token for classifying text; cause: %s", e.getMessage()), e);
        }
    }

    public static class ConfigHelper {
        private final String host;
        private final int port;
        private final String protocol;
        private final String classifierPath;
        private final String tokenEndpoint;
        private final Map<String, String> additionalParameters = new HashMap<>();

        public ConfigHelper(Context context) {
            this.host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
            this.port = context.getIntOption(Options.WRITE_CLASSIFIER_PORT, 443, 0);
            this.protocol = "true".equalsIgnoreCase(context.getStringOption(Options.WRITE_CLASSIFIER_HTTP)) ? "http" : "https";
            this.classifierPath = fixPath(context.getStringOption(Options.WRITE_CLASSIFIER_PATH));
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

        public ClassificationConfiguration buildClassificationConfiguration(String apiToken) {
            ClassificationConfiguration config = new ClassificationConfiguration();
            config.setApiToken(apiToken);
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

    private TextClassifierFactory() {
    }

    // Sonar doesn't like static assignments in this class, but this class is only used as a mock for testing.
    @SuppressWarnings("java:S2696")
    public static class MockTextClassifier implements MultiArticleClassifier {

        private final Document mockResponse;
        private static int timesInvoked;
        private static boolean wasClosed;

        // Sonar doesn't like this static assignment, but it's fine in a class that's only used as a mock.
        @SuppressWarnings("java:S3010")
        private MockTextClassifier(String mockResponse) {
            this.mockResponse = new DOMHelper(null).parseXmlString(mockResponse, null);
            timesInvoked = 0;
        }

        public static boolean isClosed() {
            return wasClosed;
        }

        public static int getTimesInvoked() {
            return timesInvoked;
        }

        @Override
        public Document classifyArticles(byte[] multiArticleDocumentBytes) {
            timesInvoked++;
            return mockResponse;
        }

        @Override
        public void close() {
            wasClosed = true;
        }
    }
}
