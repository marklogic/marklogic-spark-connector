/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.nuclia;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.Util;
import okhttp3.*;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Client for interacting with the Nuclia RAG API.
 * Handles text ingestion, processing, and retrieval of embeddings and chunks.
 * Implements AutoCloseable to properly shut down HTTP client resources.
 */
public class NucliaClient implements AutoCloseable {

    private final String apiKey;
    private final String baseUrl;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int timeoutSeconds;

    /**
     * Creates a new NucliaClient with a custom timeout.
     *
     * @param apiKey         the Nuclia API key for authentication
     * @param region         the region (e.g., "aws-us-east-2-1")
     * @param timeoutSeconds the maximum number of seconds to wait for processing to complete
     */
    public NucliaClient(String apiKey, String region, int timeoutSeconds) {
        this.apiKey = apiKey;
        this.baseUrl = "https://" + region + ".rag.progress.cloud/api/v1";
        this.httpClient = new OkHttpClient.Builder()
            .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .build();
        this.objectMapper = new ObjectMapper();
        this.timeoutSeconds = timeoutSeconds;

        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Initialized NucliaClient: baseUrl={}, timeoutSeconds={}", baseUrl, timeoutSeconds);
        }
    }

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
    public Stream<ObjectNode> processData(String filename, byte[] content) throws IOException, InterruptedException {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Starting processData; file: {}, size: {} bytes", filename, content.length);
        }

        final String resourceId = uploadFile(content, filename);
        final String processingId = submitFileForProcessing(filename, resourceId);
        final boolean completed = waitForCompletion(filename, processingId, timeoutSeconds);

        if (!completed) {
            Util.MAIN_LOGGER.warn("Processing timed out after {} seconds; file: {}, processing ID: {}. Skipping Nuclia processing for this file.",
                timeoutSeconds, filename, processingId);
            return Stream.empty();
        }

        return getProcessingResults(filename, processingId);
    }

    /**
     * Uploads a binary file to Nuclia and returns a resource ID.
     *
     * @param content  the binary content of the file
     * @param filename the name of the file
     * @return the resource ID for the uploaded file
     * @throws IOException if the upload fails
     */
    private String uploadFile(byte[] content, String filename) throws IOException {
        String endpoint = baseUrl + "/processing/upload";
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Uploading file to Nuclia: filename={}, size={} bytes", filename, content.length);
        }

        // Encode filename in base64 for X-FILENAME header
        String encodedFilename = Base64.getEncoder().encodeToString(filename.getBytes());

        Request request = newAuthenticatedRequestBuilder(endpoint)
            .header("X-FILENAME", encodedFilename)
            .post(RequestBody.create(content, null))
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                Util.MAIN_LOGGER.error("Failed to upload file: {} {}", response.code(), response.message());
                throw new IOException("Failed to upload file: " + response.code() + " " + response.message());
            }
            String resourceId = response.body().string();
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("File upload successful: filename={}", filename);
            }
            return resourceId;
        }
    }

    /**
     * Submits a file for processing using its resource ID.
     *
     * @param filename   the name of the file
     * @param resourceId the resource ID from uploadFile
     * @return the processing ID for tracking the request
     * @throws IOException if the request fails
     */
    private String submitFileForProcessing(String filename, String resourceId) throws IOException {
        final String endpoint = baseUrl + "/processing/push";
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Submitting file for processing: filename={}", filename);
        }

        final String requestBody = String.format("""
            {"filefield": {"%s": "%s"}}
            """, escapeJson(filename), escapeJson(resourceId));

        Request request = newAuthenticatedRequestBuilder(endpoint)
            .header("Content-Type", "application/json")
            .post(RequestBody.create(requestBody, MediaType.get("application/json")))
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                Util.MAIN_LOGGER.error("Failed to submit file {}: {} {}", filename, response.code(), response.message());
                throw new IOException("Failed to submit file " + filename + ": " + response.code() + " " + response.message());
            }

            String responseBody = response.body().string();
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Submit file response: {}", responseBody);
            }
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            String processingId = jsonResponse.get("processing_id").asText();
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("File submission successful: filename={}, processingId={}", filename, processingId);
            }
            return processingId;
        }
    }

    /**
     * Checks the status of a processing request.
     *
     * @param filename     the name of the file being processed
     * @param processingId the processing ID returned from submitText
     * @return true if processing is complete, false otherwise
     * @throws IOException if the request fails
     */
    private boolean isProcessingComplete(String filename, String processingId) throws IOException {
        String endpoint = baseUrl + "/processing/requests/" + processingId;

        Request request = newAuthenticatedRequestBuilder(endpoint)
            .get()
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to check status for file " + filename + ": " + response.code() + " " + response.message());
            }

            String responseBody = response.body().string();
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            return jsonResponse.has("completed") && jsonResponse.get("completed").asBoolean();
        }
    }

    /**
     * Waits for processing to complete, polling at regular intervals.
     *
     * @param filename       the name of the file being processed
     * @param processingId   the processing ID to wait for
     * @param maxWaitSeconds maximum time to wait in seconds
     * @return true if processing completed within the timeout, false otherwise
     * @throws IOException          if a request fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private boolean waitForCompletion(String filename, String processingId, int maxWaitSeconds) throws IOException, InterruptedException {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Polling for completion: filename={}, processingId={}, maxWaitSeconds={}", filename, processingId, maxWaitSeconds);
        }

        int attempts = 0;
        int maxAttempts = maxWaitSeconds;

        while (attempts < maxAttempts) {
            if (isProcessingComplete(filename, processingId)) {
                if (Util.MAIN_LOGGER.isDebugEnabled()) {
                    Util.MAIN_LOGGER.debug("Processing completed; file {}; {} attempts", filename, attempts + 1);
                }
                return true;
            }
            TimeUnit.SECONDS.sleep(1);
            attempts++;
            if (Util.MAIN_LOGGER.isDebugEnabled() && attempts % 5 == 0) {
                Util.MAIN_LOGGER.debug("Still waiting for completion: filename={}, attempt {}/{}", filename, attempts, maxAttempts);
            }
        }

        Util.MAIN_LOGGER.warn("Processing did not complete within {} seconds; file: {}", maxWaitSeconds, filename);
        return false;
    }

    /**
     * Retrieves the SSE results stream from a completed processing request.
     *
     * @param filename     the name of the file being processed
     * @param processingId the processing ID
     * @return a Stream of ObjectNode events
     * @throws IOException if the request fails
     */
    private Stream<ObjectNode> getProcessingResults(String filename, String processingId) throws IOException {
        final String endpoint = baseUrl + "/processing/requests/" + processingId + "/results";
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Retrieving SSE results: filename={}, processingId={}", filename, processingId);
        }

        Request request = newAuthenticatedRequestBuilder(endpoint)
            .get()
            .build();

        NucliaEventCollector collector = new NucliaEventCollector(objectMapper);

        EventSource eventSource = EventSources.createFactory(httpClient)
            .newEventSource(request, collector);

        try {
            Stream<ObjectNode> results = collector.awaitCompletion(timeoutSeconds).stream();
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("SSE stream completed; file: {}, processingId: {}", filename, processingId);
            }
            return results;
        } catch (InterruptedException e) {
            Util.MAIN_LOGGER.error("Interrupted while waiting for SSE stream; filename={}, processingId={}", filename, processingId, e);
            eventSource.cancel();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for SSE stream; file: " + filename, e);
        }
    }

    /**
     * Creates a new Request.Builder with the endpoint URL and Authorization header already set.
     *
     * @param endpoint the full endpoint URL
     * @return a Request.Builder with URL and auth header configured
     */
    private Request.Builder newAuthenticatedRequestBuilder(String endpoint) {
        return new Request.Builder()
            .url(endpoint)
            .header("Authorization", "Bearer " + apiKey);
    }

    /**
     * Escapes JSON special characters in a string.
     */
    private String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    /**
     * Closes the HTTP client and releases all resources.
     * Shuts down connection pools and executor services to allow JVM to exit cleanly.
     */
    @Override
    public void close() {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Closing NucliaClient and releasing HTTP client resources");
        }
        try {
            httpClient.dispatcher().executorService().shutdownNow();
            httpClient.connectionPool().evictAll();
        } finally {
            try {
                if (httpClient.cache() != null) {
                    httpClient.cache().close();
                }
            } catch (IOException e) {
                // Ignore - we're shutting down anyway
            }
        }
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("NucliaClient closed successfully");
        }
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }
}
