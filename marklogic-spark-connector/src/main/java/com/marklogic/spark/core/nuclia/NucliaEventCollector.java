/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.nuclia;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * EventSourceListener implementation that collects SSE events from Nuclia's processing results endpoint.
 * Parses each event as JSON and collects ObjectNode events into a list.
 */
class NucliaEventCollector extends EventSourceListener {

    private final ObjectMapper objectMapper;
    private final List<ObjectNode> events;
    private final CountDownLatch latch;
    private Exception error;

    NucliaEventCollector(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.events = new ArrayList<>();
        this.latch = new CountDownLatch(1);
        this.error = null;
    }

    @Override
    public void onEvent(EventSource eventSource, String id, String type, String data) {
        try {
            JsonNode node = objectMapper.readTree(data);
            if (node.isObject()) {
                events.add((ObjectNode) node);
            }
        } catch (Exception e) {
            error = e;
        }
    }

    @Override
    public void onClosed(EventSource eventSource) {
        latch.countDown();
    }

    @Override
    public void onFailure(EventSource eventSource, Throwable t, Response response) {
        error = new IOException("SSE connection failed: " +
            (response != null ? response.code() + " " + response.message() : t.getMessage()), t);
        latch.countDown();
    }

    /**
     * Waits for the SSE stream to complete.
     *
     * @param timeoutSeconds maximum time to wait in seconds
     * @return the collected list of events
     * @throws IOException if the stream fails or times out
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    List<ObjectNode> awaitCompletion(int timeoutSeconds) throws IOException, InterruptedException {
        if (!latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
            throw new IOException("SSE stream did not complete within " + timeoutSeconds + " seconds");
        }

        if (error != null) {
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException("Failed to process SSE events; cause: " + error.getMessage(), error);
        }

        return events;
    }
}
