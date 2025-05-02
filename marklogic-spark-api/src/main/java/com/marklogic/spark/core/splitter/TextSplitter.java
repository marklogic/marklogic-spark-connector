/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.List;

/**
 * Intended to hide the langchain4j details.
 */
public interface TextSplitter {

    List<String> split(String sourceUri, AbstractWriteHandle content);
}
