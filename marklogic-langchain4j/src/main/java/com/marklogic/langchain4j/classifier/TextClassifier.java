/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.*;

import java.util.HashMap;
import java.util.Map;

public class TextClassifier {

    public static final String CLASSIFICATION_MAIN_ELEMENT = "STRUCTUREDDOCUMENT";

    private static final String THRESHOLD = "20";

    private final ClassificationClient classificationClient;

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
            return classificationClient.getClassificationServerResponse(new Body(text), new Title(sourceUri));
        } catch (ClassificationException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceUri, e.getMessage()), e);
        }
    }
}
