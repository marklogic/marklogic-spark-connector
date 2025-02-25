/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationClient;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.classificationserver.client.ClassificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.IOException;

class SemaphoreMultiArticleClassifier implements MultiArticleClassifier {

    private static final Logger SEMAPHORE_LOGGER = LoggerFactory.getLogger("com.marklogic.semaphore.classifier");

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
            if (SEMAPHORE_LOGGER.isDebugEnabled()) {
                long time = (System.currentTimeMillis() - start);
                totalDurationOfCalls += time;
                if (SEMAPHORE_LOGGER.isTraceEnabled()) {
                    SEMAPHORE_LOGGER.trace("Time: {}; total duration: {}", time, totalDurationOfCalls);
                }
            }
            return response;
        } catch (ClassificationException e) {
            throw new ConnectorException(String.format("Unable to classify content; cause: %s", e.getMessage()), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (SEMAPHORE_LOGGER.isDebugEnabled() && totalDurationOfCalls > 0) {
            SEMAPHORE_LOGGER.debug("Total duration of calls: {}", totalDurationOfCalls);
        }
        if (classificationClient != null) {
            classificationClient.close();
        }
    }
}
