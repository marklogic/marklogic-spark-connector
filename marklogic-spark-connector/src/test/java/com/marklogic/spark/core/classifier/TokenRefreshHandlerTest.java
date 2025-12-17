/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.classificationserver.client.ClassificationException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TokenRefreshHandlerTest {

    @Test
    void successfulOperationWithoutTokenRefresh() throws ClassificationException {
        AtomicBoolean tokenGenerated = new AtomicBoolean(false);
        AtomicBoolean tokenSet = new AtomicBoolean(false);

        TokenRefreshHandler handler = new TokenRefreshHandler(
            () -> {
                tokenGenerated.set(true);
                return "new-token";
            },
            token -> tokenSet.set(true)
        );

        String result = handler.executeWithTokenRefresh(() -> "success", "Error: ");

        assertEquals("success", result);
        assertFalse(tokenGenerated.get(), "Token should not be regenerated on success");
        assertFalse(tokenSet.get(), "Token should not be set on success");
    }

    @Test
    void tokenRefreshOn401Error() throws ClassificationException {
        AtomicInteger tokenGenerationCount = new AtomicInteger(0);
        AtomicInteger tokenSetCount = new AtomicInteger(0);
        AtomicInteger operationCallCount = new AtomicInteger(0);

        TokenRefreshHandler handler = new TokenRefreshHandler(
            () -> {
                tokenGenerationCount.incrementAndGet();
                return "new-token-" + tokenGenerationCount.get();
            },
            token -> {
                tokenSetCount.incrementAndGet();
                assertEquals("new-token-" + tokenSetCount.get(), token);
            }
        );

        String result = handler.executeWithTokenRefresh(() -> {
            operationCallCount.incrementAndGet();
            if (operationCallCount.get() == 1) {
                throw new ClassificationException("HttpStatus: 401 received from classification server");
            }
            return "success-after-retry";
        }, "Error: ");

        assertEquals("success-after-retry", result);
        assertEquals(2, operationCallCount.get(), "Operation should be called twice (initial + retry)");
        assertEquals(1, tokenGenerationCount.get(), "Token should be regenerated once");
        assertEquals(1, tokenSetCount.get(), "Token should be set once");
    }

    @Test
    void secondFailureThrowsException() {
        AtomicInteger operationCallCount = new AtomicInteger(0);

        TokenRefreshHandler handler = new TokenRefreshHandler(
            () -> "new-token",
            token -> {
            }
        );

        ConnectorException exception = assertThrows(ConnectorException.class, () -> {
            handler.executeWithTokenRefresh(() -> {
                operationCallCount.incrementAndGet();
                throw new ClassificationException("HttpStatus: 401 received from classification server");
            }, "Unable to classify: ");
        });

        assertEquals(2, operationCallCount.get(), "Operation should be called twice before giving up");
        assertTrue(exception.getMessage().contains("Unable to classify:"));
        assertTrue(exception.getMessage().contains("401 received from classification server"));
    }

    @Test
    void non401ErrorThrownImmediately() {
        AtomicInteger tokenGenerationCount = new AtomicInteger(0);
        AtomicInteger operationCallCount = new AtomicInteger(0);

        TokenRefreshHandler handler = new TokenRefreshHandler(
            () -> {
                tokenGenerationCount.incrementAndGet();
                return "new-token";
            },
            token -> {
            }
        );

        ClassificationException exception = assertThrows(ClassificationException.class, () -> {
            handler.executeWithTokenRefresh(() -> {
                operationCallCount.incrementAndGet();
                throw new ClassificationException("Some other error");
            }, "Error: ");
        });

        assertEquals(1, operationCallCount.get(), "Operation should only be called once");
        assertEquals(0, tokenGenerationCount.get(), "Token should not be regenerated for non-401 errors");
        assertEquals("Some other error", exception.getMessage());
    }

    @Test
    void isTokenExpiredDetects401() {
        TokenRefreshHandler handler = new TokenRefreshHandler(() -> "token", token -> {
        });

        ClassificationException ex401 = new ClassificationException("Noise.. 401 Noise...");
        assertTrue(handler.isTokenExpired(ex401), "The expectation is that the presence of just '401' " +
            "in the message indicates an expired token. This avoids checking a much longer message that could change " +
            "at any time. And worst case, we may attempt a token refresh when not needed.");

        ClassificationException exOther = new ClassificationException("Some other error");
        assertFalse(handler.isTokenExpired(exOther));
    }

    @Test
    void nullTokenGeneratorDoesNotTriggerRefresh() {
        AtomicInteger operationCallCount = new AtomicInteger(0);

        TokenRefreshHandler handler = new TokenRefreshHandler(null, token -> {
        });

        ClassificationException exception = assertThrows(ClassificationException.class, () -> {
            handler.executeWithTokenRefresh(() -> {
                operationCallCount.incrementAndGet();
                throw new ClassificationException("HttpStatus: 401 received from classification server");
            }, "Error: ");
        });

        assertEquals(1, operationCallCount.get(), "Operation should only be called once when token generator is null");
        assertTrue(exception.getMessage().contains("401 received from classification server"));
    }
}
