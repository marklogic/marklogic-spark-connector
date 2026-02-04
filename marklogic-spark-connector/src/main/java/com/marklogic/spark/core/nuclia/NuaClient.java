/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.nuclia;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * Client interface for interacting with the Nuclia Understanding API (NUA).
 * Handles text ingestion, processing, and retrieval of embeddings and chunks.
 *
 * @since 3.1.0
 */
public interface NuaClient extends Closeable {

    /**
     * Processes a file through Nuclia's pipeline synchronously.
     * Uploads the file, submits for processing, waits for completion, and retrieves the results.
     *
     * @param filename the name of the file
     * @param content  the binary content of the file
     * @return a Stream of ObjectNode events containing chunks and embeddings
     * @throws IOException          if any request fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    Stream<ObjectNode> processData(String filename, byte[] content) throws IOException, InterruptedException;

    /**
     * Returns the base URL of the Nuclia API endpoint.
     *
     * @return the base URL
     */
    String getBaseUrl();

    /**
     * Returns the configured timeout in seconds.
     *
     * @return the timeout in seconds
     */
    int getTimeoutSeconds();
}
