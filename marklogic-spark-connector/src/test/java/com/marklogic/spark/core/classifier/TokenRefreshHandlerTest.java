/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

        // Exact server format
        assertTrue(handler.isTokenExpired(new ClassificationException("401 received from classification server")));
        // "401" anywhere in the message, delimited by non-word characters (word-boundary match)
        assertTrue(handler.isTokenExpired(new ClassificationException("Noise.. 401 Noise...")));

        assertFalse(handler.isTokenExpired(new ClassificationException("Some other error")));
    }

    @Test
    void partialNumberDoesNotTriggerTokenRefresh() {
        // "4010" contains "401" as a substring but must NOT be treated as an HTTP 401.
        TokenRefreshHandler handler = new TokenRefreshHandler(() -> "token", token -> {
        });

        assertFalse(handler.isTokenExpired(new ClassificationException("Error 4010: invalid parameter")),
            "A message containing '401' only as part of a longer number should not trigger a token refresh");
        assertFalse(handler.isTokenExpired(new ClassificationException("4010")),
            "'4010' with no surrounding text should not match the word-boundary pattern for 401");
        assertFalse(handler.isTokenExpired(new ClassificationException("x4010x")),
            "'4010' embedded in alphanumeric text should not trigger a token refresh");
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
