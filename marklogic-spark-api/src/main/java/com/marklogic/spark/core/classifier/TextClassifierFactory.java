/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */

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
import com.smartlogic.cloud.Token;
import com.smartlogic.cloud.TokenFetcher;
import org.w3c.dom.Document;

import javax.validation.constraints.NotNull;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class TextClassifierFactory {

    private static final String MOCK_CLASSIFIER_OPTION = "spark.marklogic.testing.mockClassifierResponse";

    public static TextClassifier newTextClassifier(Context context) {
        SemaphoreProxy semaphoreProxy = null;
        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);

        if (context.hasOption(MOCK_CLASSIFIER_OPTION)) {
            String mockResponse = context.getStringOption(MOCK_CLASSIFIER_OPTION);
            Objects.requireNonNull(mockResponse);
            semaphoreProxy = new MockSemaphoreProxy(mockResponse);
        } else if (host != null && !host.trim().isEmpty()) {
            try {
                ClassificationConfiguration config = buildClassificationConfiguration(context);
                semaphoreProxy = new SemaphoreProxyImpl(config);
            } catch (ConnectorException ex) {
                throw ex;
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to configure a connection for classifying text; cause: %s",
                    e.getMessage()), e);
            }
        }

        if (semaphoreProxy != null) {
            // We may need a dedicated encoding for this
            String encoding = context.getStringOption(Options.READ_FILES_ENCODING, "UTF-8");
            int batchSize = context.getIntOption(Options.WRITE_CLASSIFIER_BATCH_SIZE, 20, 1);
            return new SemaphoreTextClassifier(semaphoreProxy, encoding, batchSize);
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
            Token token = tokenFetcher.getAccessToken();
            Objects.requireNonNull(token);
            return token.getAccess_token();
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
    public static class MockSemaphoreProxy implements SemaphoreProxy {

        private final Document mockResponse;
        private static int timesInvoked;
        private static boolean wasClosed;

        // Sonar doesn't like this static assignment, but it's fine in a class that's only used as a mock.
        @SuppressWarnings("java:S3010")
        private MockSemaphoreProxy(@NotNull String mockResponse) {
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
        public byte[] classifyDocument(byte[] content, String uri) {
            String mockSingleArticleResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "            <response>\n" +
                "              <STRUCTUREDDOCUMENT>\n" +
                "              <URL>../tmp/ca002056-e3f6-4c81-8c9f-00ca218330c4/1739460469_43eb</URL>\n" +
                "              <SYSTEM name=\"HASH\" value=\"2c3bcaf41fbabf8ff2e236c7580893ec\"/>\n" +
                "              <META name=\"Type\" value=\"TEXT (4003)\"/>\n" +
                "              <META name=\"title/document_title\" value=\"/some-uri.xml\"/>\n" +
                "              <SYSTEM name=\"DeterminedLanguage\" value=\"default\"/>" +
                "</STRUCTUREDDOCUMENT></response>\n";

            return mockSingleArticleResponse.getBytes();
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
