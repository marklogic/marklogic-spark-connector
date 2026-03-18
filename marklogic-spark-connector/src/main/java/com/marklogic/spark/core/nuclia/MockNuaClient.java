/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.nuclia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Mock implementation of {@link NuaClient} for testing purposes.
 * Returns canned JSON responses without making actual API calls to Nuclia.
 *
 * @since 3.1.0
 */
// Sonar doesn't like static assignments in this class, but this class is only used as a mock for testing.
@SuppressWarnings("java:S2696")
public record MockNuaClient(String mockResponseJson) implements NuaClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Stream<ObjectNode> processData(String filename, byte[] content) throws IOException {
        ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(mockResponseJson);
        // Simulate the SSE stream by returning the same node twice.
        return Stream.of(node, node);
    }

    @Override
    public String getBaseUrl() {
        return "https://example.org/doesnt-matter";
    }

    @Override
    public int getTimeoutSeconds() {
        return 0;
    }

    @Override
    public void close() {
    }
}
