/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.langchain4j.classifier.TextClassifier;
import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.Token;
import com.smartlogic.cloud.TokenFetcher;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public abstract class TextClassifierUdf {
    private static final String THRESHOLD = "20";

    private static ClassificationConfiguration classificationConfiguration;

    public static UserDefinedFunction build(
            String host,boolean https, String port, String endpoint, String apiKey, String tokenEndpoint
    ) {
        String protocol = https ? "https" : "http";
        String tokenUrl = protocol + "://" + host + ":" + port + "/" + tokenEndpoint;
        TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, apiKey);
        Token token;
        try {
            token = tokenFetcher.getAccessToken();
        } catch (CloudException e) {
            throw new ConnectorException(String.format("Unable to initialize Classifier UDF; cause: %s", e.getMessage()), e);
        }

        classificationConfiguration = new ClassificationConfiguration();
        classificationConfiguration.setSingleArticle(false);
        classificationConfiguration.setMultiArticle(true);
        classificationConfiguration.setSocketTimeoutMS(100000);
        classificationConfiguration.setConnectionTimeoutMS(100000);
        classificationConfiguration.setApiToken(token.getAccess_token());
        classificationConfiguration.setProtocol(protocol);
        classificationConfiguration.setHostName(host);
        classificationConfiguration.setHostPort(Integer.parseInt(port));
        classificationConfiguration.setHostPath(endpoint);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put("threshold", THRESHOLD);
        additionalParameters.put("language", "en1");
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        return functions.udf(TextClassifierUdf::classifyText, DataTypes.BinaryType);
    }

    private static byte[] classifyText(Object binaryContent) {
        if (!(binaryContent instanceof byte[])) {
            throw new IllegalArgumentException(
                    "Text classification UDF must be run against a column containing non-null byte arrays."
            );
        }
        // Need to add handler for when a token expires midstream.
        String content = new String((byte[]) binaryContent, StandardCharsets.UTF_8);
        TextClassifier textClassifier = new TextClassifier(classificationConfiguration);
        return textClassifier.classifyTextToBytes("sourceUri", content);
    }

    private TextClassifierUdf() {
    }
}
