/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.spark.core.classifier.TextClassifier;
import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.TokenFetcher;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TextClassifierUdf {
    private static final String THRESHOLD = "20";

    private static ClassificationConfiguration classificationConfiguration;

    public static UserDefinedFunction build(
            String host,boolean https, String port, String endpoint, String apiKey, String tokenEndpoint
    ) {
        String protocol = https ? "https" : "http";
        classificationConfiguration = new ClassificationConfiguration();
        classificationConfiguration.setSingleArticle(false);
        classificationConfiguration.setMultiArticle(true);
        classificationConfiguration.setSocketTimeoutMS(100000);
        classificationConfiguration.setConnectionTimeoutMS(100000);
        classificationConfiguration.setProtocol(protocol);
        classificationConfiguration.setHostName(host);
        classificationConfiguration.setHostPort(Integer.parseInt(port));
        classificationConfiguration.setHostPath(endpoint);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put("threshold", THRESHOLD);
        additionalParameters.put("language", "en1");
        additionalParameters.put("apiKey", apiKey);
        additionalParameters.put("tokenEndpoint", tokenEndpoint);
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        return functions.udf(TextClassifierUdf::classifyText, DataTypes.createArrayType(DataTypes.StringType));
    }

    private static List<String> classifyText(Object binaryContent) {
        // Need to add handler for when a token expires midstream.
        if (binaryContent instanceof byte[]) {
            fetchTokenIfNecessary();
            List<String> classifications = new ArrayList<>();
            String content = new String((byte[]) binaryContent, StandardCharsets.UTF_8);
            TextClassifier textClassifier = new TextClassifier(classificationConfiguration);
            classifications.add(new String(textClassifier.classifyTextToBytes("sourceUri", content), StandardCharsets.UTF_8));
            return classifications;
        } else if (binaryContent instanceof WrappedArray) {
            fetchTokenIfNecessary();
            List<String> classifications = new ArrayList<>();
            WrappedArray<String> chunks = (WrappedArray<String>) binaryContent;
            chunks.foreach((String content) -> {
                TextClassifier textClassifier = new TextClassifier(classificationConfiguration);
                byte[] response = textClassifier.classifyTextToBytes("sourceUri", content);
                classifications.add(new String(response));
                return null;
            });
            return classifications;
        } else {
            throw new IllegalArgumentException(
                "Text classification UDF must be run against a column containing non-null byte arrays."
            );
        }
    }

    private static void fetchTokenIfNecessary() {
        if (classificationConfiguration.getApiToken() == null) {
            try {
                String tokenUrl = classificationConfiguration.getProtocol() + "://" + classificationConfiguration.getHostName() + ":" + classificationConfiguration.getHostPort() + "/" + classificationConfiguration.getAdditionalParameters().get("tokenEndpoint");
                TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, classificationConfiguration.getAdditionalParameters().get("apiKey"));
                classificationConfiguration.setApiToken(tokenFetcher.getAccessToken().getAccess_token());
            } catch (CloudException e) {
                throw new ConnectorException(String.format("Unable to initialize Classifier UDF; cause: %s", e.getMessage()), e);
            }
        }
    }

    private TextClassifierUdf() {
    }

    public static ClassificationConfiguration getClassificationConfiguration() {
        return classificationConfiguration;
    }
}
