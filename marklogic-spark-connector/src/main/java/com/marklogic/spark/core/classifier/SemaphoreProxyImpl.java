/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationClient;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.classificationserver.client.ClassificationException;
import org.w3c.dom.Document;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Not threadsafe, as the config of the underlying client is modified. This is fine in the context of the connector,
 * where it is only used by a single Spark data writer.
 */
class SemaphoreProxyImpl implements SemaphoreProxy {

    private final ClassificationClient classificationClient;
    private final TokenRefreshHandler tokenRefreshHandler;
    private long totalDurationOfCalls;

    // A Supplier<String> is used instead of TokenGenerator so that we can easily mock it in tests for the
    // TokenRefreshHandler.
    SemaphoreProxyImpl(ClassificationConfiguration config, Supplier<String> tokenGenerator) {
        this.classificationClient = new ClassificationClient();
        if (tokenGenerator != null) {
            config.setApiToken(tokenGenerator.get());
            this.tokenRefreshHandler = new TokenRefreshHandler(
                tokenGenerator,
                token -> classificationClient.getClassificationConfiguration().setApiToken(token)
            );
        } else {
            this.tokenRefreshHandler = null;
        }
        config.setMultiArticle(true);
        classificationClient.setClassificationConfiguration(config);
    }

    @Override
    public byte[] classifyDocument(byte[] content, String uri) {
        try {
            classificationClient.getClassificationConfiguration().setMultiArticle(false);
            if (tokenRefreshHandler != null) {
                return tokenRefreshHandler.executeWithTokenRefresh(
                    () -> doClassifyDocument(content, uri),
                    String.format("Unable to classify content with URI: %s; cause: ", uri)
                );
            }
            return doClassifyDocument(content, uri);
        } catch (ClassificationException ex) {
            throw new ConnectorException(String.format("Unable to classify content with URI: %s; cause: %s", uri, ex.getMessage()), ex);
        } finally {
            classificationClient.getClassificationConfiguration().setMultiArticle(true);
        }
    }

    private byte[] doClassifyDocument(byte[] content, String uri) throws ClassificationException {
        long start = System.currentTimeMillis();
        byte[] response = classificationClient.getClassificationServerResponse(content, uri, null, null);
        logDuration(start);
        return response;
    }

    @Override
    public Document classifyArticles(byte[] multiArticleDocumentBytes) {
        try {
            if (tokenRefreshHandler != null) {
                return tokenRefreshHandler.executeWithTokenRefresh(
                    () -> doClassifyArticles(multiArticleDocumentBytes),
                    "Unable to classify content; cause: "
                );
            }
            return doClassifyArticles(multiArticleDocumentBytes);
        } catch (ClassificationException ex) {
            throw new ConnectorException(String.format("Unable to classify content; cause: %s", ex.getMessage()), ex);
        }
    }

    private Document doClassifyArticles(byte[] multiArticleDocumentBytes) throws ClassificationException {
        long start = System.currentTimeMillis();
        Document response = classificationClient.getStructuredDocument(multiArticleDocumentBytes, "content.xml");
        logDuration(start);
        return response;
    }

    @Override
    public void close() throws IOException {
        if (SemaphoreTextClassifier.SEMAPHORE_LOGGER.isDebugEnabled() && totalDurationOfCalls > 0) {
            SemaphoreTextClassifier.SEMAPHORE_LOGGER.debug("Total duration of calls: {}", totalDurationOfCalls);
        }
        if (classificationClient != null) {
            classificationClient.close();
        }
    }

    private void logDuration(long start) {
        if (SemaphoreTextClassifier.SEMAPHORE_LOGGER.isDebugEnabled()) {
            long time = (System.currentTimeMillis() - start);
            totalDurationOfCalls += time;
            if (SemaphoreTextClassifier.SEMAPHORE_LOGGER.isTraceEnabled()) {
                SemaphoreTextClassifier.SEMAPHORE_LOGGER.trace("Time: {}; total duration: {}", time, totalDurationOfCalls);
            }
        }
    }
}
