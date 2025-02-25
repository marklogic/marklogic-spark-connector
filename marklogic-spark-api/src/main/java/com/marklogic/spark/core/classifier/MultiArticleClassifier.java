/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import org.w3c.dom.Document;

import java.io.Closeable;

/**
 * Provides an abstraction over the call to Semaphore that sends a document containing multiple articles and
 * then receives a document containing a classification for each article.
 */
public interface MultiArticleClassifier extends Closeable {

    Document classifyArticles(byte[] multiArticleDocumentBytes);
}
