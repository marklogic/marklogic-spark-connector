/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import java.io.Closeable;

public interface TextClassifier extends Closeable {

    byte[] classifyText(String sourceUri, String text);

}
