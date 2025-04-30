/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationClient;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.classificationserver.client.ClassificationException;
import org.w3c.dom.Document;

import java.io.IOException;

/**
 * Not threadsafe, as the config of the underlying client is modified. This is fine in the context of the connector,
 * where it is only used by a single Spark data writer.
 */
class SemaphoreProxyImpl implements SemaphoreProxy {

    private final ClassificationClient classificationClient;
    private long totalDurationOfCalls;

    public SemaphoreProxyImpl(ClassificationConfiguration config) {
        this.classificationClient = new ClassificationClient();
        config.setMultiArticle(true);
        classificationClient.setClassificationConfiguration(config);
    }

    @Override
    public byte[] classifyDocument(byte[] content, String uri) {
        try {
            classificationClient.getClassificationConfiguration().setMultiArticle(false);
            long start = System.currentTimeMillis();
            byte[] response = classificationClient.getClassificationServerResponse(content, uri, null, null);
            logDuration(start);
            return response;
        } catch (ClassificationException ex) {
            throw new ConnectorException(String.format("Unable to classify content with URI: %s; cause: %s", uri, ex.getMessage()), ex);
        } finally {
            classificationClient.getClassificationConfiguration().setMultiArticle(true);
        }
    }

    @Override
    public Document classifyArticles(byte[] multiArticleDocumentBytes) {
        try {
            long start = System.currentTimeMillis();
            Document response = classificationClient.getStructuredDocument(multiArticleDocumentBytes, "content.xml");
            logDuration(start);
            return response;
        } catch (ClassificationException e) {
            throw new ConnectorException(String.format("Unable to classify content; cause: %s", e.getMessage()), e);
        }
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
