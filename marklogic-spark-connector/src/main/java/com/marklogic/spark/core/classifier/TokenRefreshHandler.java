/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationException;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Handles token refresh logic when a 401 error is received from the classification server.
 * This class is separate from SemaphoreProxyImpl to enable unit testing without needing to mock
 * the ClassificationClient.
 */
record TokenRefreshHandler(Supplier<String> tokenGenerator, Consumer<String> tokenSetter) {

    <T> T executeWithTokenRefresh(SupplierWithException<T> operation, String errorContext) throws ClassificationException {
        try {
            return operation.get();
        } catch (ClassificationException ex) {
            if (isTokenExpired(ex)) {
                SemaphoreTextClassifier.SEMAPHORE_LOGGER.info("Received 401 from classification server; generating new API token.");
                refreshToken();
                return retryOperation(operation, errorContext);
            }
            throw ex;
        }
    }

    boolean isTokenExpired(ClassificationException ex) {
        // The Classification server currently - December 2025 - responds with "401 received from classification server".
        // Looking for that entire message is likely brittle as it could change at any time. So just checking for the
        // presence of "401". Worst case, we may attempt a token refresh when not needed.
        return tokenGenerator != null &&
            ex.getMessage() != null &&
            ex.getMessage().contains("401");
    }

    private void refreshToken() {
        String newToken = tokenGenerator.get();
        // This should never happen, but Polaris is complaining - so, just in case it somehow does...
        Objects.requireNonNull(newToken, "Token generation unexpectedly returned a null token.");
        tokenSetter.accept(newToken);
    }

    private <T> T retryOperation(SupplierWithException<T> operation, String errorContext) throws ClassificationException {
        try {
            return operation.get();
        } catch (ClassificationException ex) {
            throw new ConnectorException(errorContext + ex.getMessage(), ex);
        }
    }

    @FunctionalInterface
    interface SupplierWithException<T> {
        T get() throws ClassificationException;
    }
}
