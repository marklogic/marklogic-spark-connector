/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.net.SocketException;

public class IOExceptionTestExecutionListener implements TestExecutionExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IOExceptionTestExecutionListener.class);

    // Telling Sonar not to complain about the intentional sleep.
    @SuppressWarnings("java:S2925")
    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        logger.error("Handling error; class: {}; message: {}", throwable.getClass(), throwable.getMessage());
        Throwable cause = throwable.getCause();
        if (cause instanceof InterruptedIOException || cause instanceof SocketException) {
            logger.error("Cause class: {}; message: {}; assuming MarkLogic is being restarted on Docker and not throwing this error.",
                cause.getClass(), cause.getMessage());
            Thread.sleep(1000);
        } else {
            logger.error("Unexpected error, swallowing for now");
        }
    }
}
