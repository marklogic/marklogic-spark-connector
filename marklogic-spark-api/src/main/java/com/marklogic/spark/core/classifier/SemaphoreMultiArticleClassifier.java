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

class SemaphoreMultiArticleClassifier implements MultiArticleClassifier {

    private final ClassificationClient classificationClient;
    private long totalDurationOfCalls;

    public SemaphoreMultiArticleClassifier(ClassificationConfiguration config) {
        this.classificationClient = new ClassificationClient();
        config.setMultiArticle(true);
        classificationClient.setClassificationConfiguration(config);
    }

    @Override
    public Document classifyArticles(byte[] multiArticleDocumentBytes) {
        try {
            long start = System.currentTimeMillis();
            Document response = classificationClient.getStructuredDocument(multiArticleDocumentBytes, "content.xml");
            if (SemaphoreTextClassifier.SEMAPHORE_LOGGER.isDebugEnabled()) {
                long time = (System.currentTimeMillis() - start);
                totalDurationOfCalls += time;
                if (SemaphoreTextClassifier.SEMAPHORE_LOGGER.isTraceEnabled()) {
                    SemaphoreTextClassifier.SEMAPHORE_LOGGER.trace("Time: {}; total duration: {}", time, totalDurationOfCalls);
                }
            }
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
}
