/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import org.w3c.dom.Document;

import java.io.Closeable;

/**
 * Provides an abstraction over the call to Semaphore that sends a document containing multiple articles and
 * then receives a document containing a classification for each article.
 *
 * This will get a new name soon now that its scope is more than just classifying a set of articles.
 */
public interface MultiArticleClassifier extends Closeable {

    byte[] classifyDocument(byte[] content, String uri);

    Document classifyArticles(byte[] multiArticleDocumentBytes);
}
