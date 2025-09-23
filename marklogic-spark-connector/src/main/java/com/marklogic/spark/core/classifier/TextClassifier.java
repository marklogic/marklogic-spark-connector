/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.core.DocumentInputs;

import java.io.Closeable;
import java.util.List;

/**
 * Main interface for the text classification system; intended to hide the use of Semaphore.
 */
public interface TextClassifier extends Closeable {

    /**
     * Classify the content of a single document. This will always result in a single document being sent to Semaphore
     * in a single request, which is typically not ideal for performance. However, combining 2+ documents into a single
     * "multi-article" request can be problematic when the document types differ.
     *
     * @param inputs
     */
    void classifyDocument(DocumentInputs inputs);

    /**
     * Classify the text in each object, with the classification being added to each associated object. This approach
     * will leverage a multi-article request to Semaphore, which is more efficient than classifying each document one
     * at a time. A multi-article request is possible here because we know how to get the text from each chunk and
     * thus don't have to worry about whether the document is JSON, XML, or a binary.
     *
     * @param classifiableContents
     */
    void classifyChunks(List<ClassifiableContent> classifiableContents);

    /**
     * Abstraction to hide whether a document is being classified or an individual chunk is being classified. For the
     * classifier, it doesn't matter in terms of batching up requests.
     * <p>
     * This abstraction currently doesn't matter because there's only one implementation. We used to have a second
     * implementation based on an entire document. But we ran into errors when trying to e.g. classify both a binary
     * and JSON document in the same multi-article request. For that reason, each document is classified by itself for
     * now. And this interface is thus only ever implemented for a chunk of text. This abstraction is being kept in
     * hopes that we can support entire documents in the future.
     */
    interface ClassifiableContent {

        String getTextToClassify();

        void addClassification(byte[] classification);
    }
}
