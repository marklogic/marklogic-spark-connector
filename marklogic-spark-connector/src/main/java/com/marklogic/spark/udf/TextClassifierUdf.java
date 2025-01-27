/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.udf;

import com.marklogic.langchain4j.classifier.TextClassifier;
import com.smartlogic.cloud.CloudException;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.nio.charset.StandardCharsets;

public abstract class TextClassifierUdf {

    private static String host;
    private static Boolean https;
    private static String port;
    private static String endpoint;
    private static String apiKey;
    private static String tokenEndpoint;

    public static UserDefinedFunction build(
        String host, Boolean https, String port, String endpoint, String apiKey, String tokenEndpoint
    ) {
        TextClassifierUdf.host = host;
        TextClassifierUdf.https = https;
        TextClassifierUdf.port = port;
        TextClassifierUdf.endpoint = endpoint;
        TextClassifierUdf.apiKey = apiKey;
        TextClassifierUdf.tokenEndpoint = tokenEndpoint;
        return functions.udf(TextClassifierUdf::classifyText, DataTypes.BinaryType);
    }

    private static byte[] classifyText(Object binaryContent) throws CloudException {
        if (!(binaryContent instanceof byte[])) {
            throw new IllegalArgumentException(
                "Text classification UDF must be run against a column containing non-null byte arrays."
            );
        }
        // Probably need to refactor the TextClassifier so that the token can be generated outside of that class,
        // and passed into the constructor, so that it isn't generated on every classification call.
        String content = new String((byte[]) binaryContent, StandardCharsets.UTF_8);
        TextClassifier textClassifier = new TextClassifier(host, https.toString(), port, endpoint, apiKey, tokenEndpoint);
        return textClassifier.classifyTextToBytes("sourceUri", content);
    }

    private TextClassifierUdf() {
    }
}
