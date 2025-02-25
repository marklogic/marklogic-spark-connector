/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import java.io.Closeable;
import java.util.List;

/**
 * Main interface for the text classification system; intended to hide the use of Semaphore.
 */
public interface TextClassifier extends Closeable {

    /**
     * Classify the text in each object, with the classification being added to each associated object.
     *
     * @param classifiableContents
     */
    void classifyText(List<ClassifiableContent> classifiableContents);

    /**
     * Abstraction to hide whether a document is being classified or an individual chunk is being classified. For the
     * classifier, it doesn't matter in terms of batching up requests.
     */
    interface ClassifiableContent {

        String getTextToClassify();

        void addClassification(byte[] classification);
    }
}
