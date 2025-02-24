/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import java.io.Closeable;
import java.util.List;

public interface TextClassifier extends Closeable {

    // This will be removed soon in favor of the below method.
    byte[] classifyText(String sourceUri, String text);

    void classifyText(List<ClassifiableContent> classifiableContents);

    /**
     * Abstraction to hide whether a document is being classified or an individual chunk is being classified. For the
     * classifier, it doesn't matter in terms of batching up requests.
     */
    interface ClassifiableContent {

        String getSourceUri();

        String getTextToClassify();

        void addClassification(byte[] classification);
    }
}
