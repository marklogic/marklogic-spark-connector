/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.Util;
import org.apache.jena.riot.system.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from MLCP. Once RDF writing is supported, need to test via the files referenced in
 * https://progresssoftware.atlassian.net/browse/MLE-2133 .
 */
class RdfErrorHandler implements ErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(RdfErrorHandler.class);

    private final String path;

    RdfErrorHandler(String path) {
        this.path = path;
    }

    @Override
    public void warning(String message, long line, long col) {
        boolean isDebugMessage = message.contains("Bad IRI:") || message.contains("Illegal character in IRI") || message.contains("Not advised IRI");
        if (isDebugMessage) {
            if (logger.isDebugEnabled()) {
                logger.debug(formatMessage(message, line, col));
            }
        } else if (Util.MAIN_LOGGER.isWarnEnabled()) {
            Util.MAIN_LOGGER.warn(formatMessage(message, line, col));
        }
    }

    @Override
    public void error(String message, long line, long col) {
        boolean isDebugMessage = message.contains("Bad character in IRI") || message.contains("Problem setting StAX property");
        if (isDebugMessage) {
            if (logger.isDebugEnabled()) {
                logger.debug(formatMessage(message, line, col));
            }
        } else if (Util.MAIN_LOGGER.isErrorEnabled()) {
            Util.MAIN_LOGGER.error(formatMessage(message, line, col));
        }
    }

    @Override
    public void fatal(String message, long line, long col) {
        if (Util.MAIN_LOGGER.isErrorEnabled()) {
            Util.MAIN_LOGGER.error(formatMessage(message, line, col));
        }
    }

    private String formatMessage(String message, long line, long col) {
        String preamble = this.path + ":";
        if (line >= 0) {
            preamble += line;
            if (col >= 0) {
                preamble += ":" + col;
            }
        }
        return preamble + " " + message;
    }
}
