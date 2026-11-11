/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import org.w3c.dom.Document;

import java.io.Closeable;

/**
 * Provides an abstraction over the call to Semaphore that sends a document containing multiple articles and
 * then receives a document containing a classification for each article. Main use case is to enable easy mocking of
 * the calls to Semaphore for testing purposes.
 */
public interface SemaphoreProxy extends Closeable {

    /**
     * @param content  bytes to classify
     * @param fileName bounded, safe name sent to Classification Server (used by its TypeGuesser)
     * @param uri      original document URI, used only in error messages
     */
    byte[] classifyDocument(byte[] content, String fileName, String uri);

    Document classifyArticles(byte[] multiArticleDocumentBytes);
}
