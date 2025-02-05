/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Telling Sonar to ignore this for now, will refactor it soon.
@SuppressWarnings("java:S1214")
public interface Util {

    /**
     * Intended for log messages pertaining to the embedder feature. Uses a separate logger so that it can be enabled
     * at the info/debug level without enabling any other log messages.
     */
    Logger LANGCHAIN4J_LOGGER = LoggerFactory.getLogger("com.marklogic.langchain4j");

}
