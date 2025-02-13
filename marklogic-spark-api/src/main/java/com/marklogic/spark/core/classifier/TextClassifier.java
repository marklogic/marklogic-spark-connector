/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

public interface TextClassifier {

    byte[] classifyText(String sourceUri, String text);

}
