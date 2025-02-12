/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.smartlogic.classificationserver.client.*;

import java.util.HashMap;
import java.util.Map;

public class TextClassifier {

    public static final String CLASSIFICATION_MAIN_ELEMENT = "STRUCTUREDDOCUMENT";

    private static final String THRESHOLD = "20";

    private final ClassificationClient classificationClient;
    private long totalDurationOfCalls;

    public TextClassifier(ClassificationConfiguration classificationConfiguration) {
        this.classificationClient = new ClassificationClient();
        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put("threshold", THRESHOLD);
        additionalParameters.put("language", "en1");
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        classificationClient.setClassificationConfiguration(classificationConfiguration);
    }

    public byte[] classifyTextToBytes(String sourceUri, String text) {
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
