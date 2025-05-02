/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.NotSerializableException;

public class IOExceptionTestExecutionListener implements TestExecutionExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IOExceptionTestExecutionListener.class);

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        // Valid test failures should still be thrown. We're just looking to catch intermittent IO exceptions.
        if (throwable instanceof AssertionError) {
            throw throwable;
        }

        if (!logIfIOException(throwable)) {
            throw throwable;
        }
    }

    // Telling Sonar not to complain about the intentional sleep.
    @SuppressWarnings("java:S2925")
    private boolean logIfIOException(Throwable throwable) {
        if (throwable instanceof IOException && !(throwable instanceof NotSerializableException)) {
            logger.error("Cause is IOException: {}; assuming this is due to a temporary and intermittent connection " +
                "issue due to MarkLogic running on Docker, so will not fail test.", throwable.getMessage());

            // Sleep for a short duration in case MarkLogic was in fact restarted by Docker.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignore.
            }
            return true;
        } else if (throwable.getCause() != null) {
            return logIfIOException(throwable.getCause());
        }
        return false;
    }
}
