
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
        } else if (host != null && host.trim().length() > 0) {
            try {
                ClassificationConfiguration config = buildClassificationConfiguration(context);
                multiArticleClassifier = new SemaphoreMultiArticleClassifier(config);
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to configure a connection for classifying text; cause: %s", e.getMessage()));
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

            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Will classify text via host {}; token URL: {}", this.host, this.tokenUrl);
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
