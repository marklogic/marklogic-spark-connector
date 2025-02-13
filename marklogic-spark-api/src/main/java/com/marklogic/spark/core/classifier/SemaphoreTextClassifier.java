/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.smartlogic.classificationserver.client.*;

class SemaphoreTextClassifier implements TextClassifier {

    private final ClassificationClient classificationClient;
    private long totalDurationOfCalls;

    SemaphoreTextClassifier(ClassificationConfiguration classificationConfiguration) {
        this.classificationClient = new ClassificationClient();
        classificationClient.setClassificationConfiguration(classificationConfiguration);
    }

    @Override
    public byte[] classifyText(String sourceUri, String text) {
        try {
            long start = System.currentTimeMillis();
            byte[] response = classificationClient.getClassificationServerResponse(new Body(text), new Title(sourceUri));
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                long time = (System.currentTimeMillis() - start);
                totalDurationOfCalls += time;
                Util.MAIN_LOGGER.debug("Source URI: {}; time: {}; total duration: {}", sourceUri, time, totalDurationOfCalls);
            }
            return response;
        } catch (ClassificationException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceUri, e.getMessage()), e);
        }
    }
}
