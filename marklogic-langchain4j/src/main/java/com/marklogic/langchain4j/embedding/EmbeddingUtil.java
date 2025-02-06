/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class EmbeddingUtil {

    /**
     * Intended for log messages pertaining to the embedder feature. Uses a separate logger so that it can be enabled
     * at the info/debug level without enabling any other log messages.
     */
    static final Logger LANGCHAIN4J_LOGGER = LoggerFactory.getLogger("com.marklogic.langchain4j");

    private EmbeddingUtil() {
    }
}
