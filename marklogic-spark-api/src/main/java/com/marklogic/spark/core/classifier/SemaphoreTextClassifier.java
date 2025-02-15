/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SemaphoreTextClassifier implements TextClassifier {

    protected static final Logger CLASSIFIER_LOGGER = LoggerFactory.getLogger("com.marklogic.semaphore.classifier");

    private final ClassificationClient classificationClient;
    private long totalDurationOfCalls;

    SemaphoreTextClassifier(ClassificationConfiguration config) {
        this.classificationClient = new ClassificationClient();
        classificationClient.setClassificationConfiguration(config);
    }

    @Override
    public byte[] classifyText(String sourceUri, String text) {
        try {
            long start = System.currentTimeMillis();
            byte[] response = classificationClient.getClassificationServerResponse(new Body(text), new Title(sourceUri));
            if (CLASSIFIER_LOGGER.isDebugEnabled()) {
                long time = (System.currentTimeMillis() - start);
                totalDurationOfCalls += time;
                if (CLASSIFIER_LOGGER.isTraceEnabled()) {
                    CLASSIFIER_LOGGER.trace("Source URI: {}; time: {}; total duration: {}", sourceUri, time, totalDurationOfCalls);
                }
            }
            return response;
        } catch (ClassificationException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceUri, e.getMessage()), e);
        }
    }

    @Override
    public void close() {
        if (CLASSIFIER_LOGGER.isDebugEnabled() && totalDurationOfCalls > 0) {
            CLASSIFIER_LOGGER.debug("Total duration of calls: {}", totalDurationOfCalls);
        }
        if (classificationClient != null) {
            classificationClient.close();
        }
    }
}
