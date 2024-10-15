/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentByRegexSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;

public abstract class DocumentSplitterFactory {

    public static DocumentSplitter makeDocumentSplitter(ContextSupport context) {
        try {
            if (context.hasOption(Options.WRITE_SPLITTER_REGEX)) {
                return makeRegexSplitter(context);
            }
            return makeDefaultSplitter(context);
        } catch (IllegalArgumentException ex) {
            String message = ex.getMessage();
            if (message != null) {
                message = massageLangchain4jError(context, message);
            }
            // Not including the underlying error so that the langchain4j details aren't exposed to the user.
            throw new ConnectorException(String.format("Unable to create splitter for documents; cause: %s", message));
        }
    }

    private static DocumentSplitter makeDefaultSplitter(ContextSupport context) {
        return DocumentSplitters.recursive(
            getMaxChunkSize(context),
            getMaxOverlapSize(context)
        );
    }

    private static DocumentSplitter makeRegexSplitter(ContextSupport context) {
        String regex = context.getStringOption(Options.WRITE_SPLITTER_REGEX);
        String joinDelimiter = context.getStringOption(Options.WRITE_SPLITTER_JOIN_DELIMITER, " ");
        DocumentSplitter splitter = new DocumentByRegexSplitter(regex, joinDelimiter, getMaxChunkSize(context), getMaxOverlapSize(context));
        // Test the splitter to ensure an invalid regex causes an immediate failure.
        try {
            splitter.split(new Document("Test data"));
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Cannot split documents due to invalid regex: %s; cause: %s", regex, e.getMessage()), e);
        }
        return splitter;
    }

    private static int getMaxChunkSize(ContextSupport context) {
        return context.getIntOption(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000, 0);
    }

    private static int getMaxOverlapSize(ContextSupport context) {
        return context.getIntOption(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0, 0);
    }

    /**
     * langchain4j does a nice job with validating inputs, but we don't want langchain4j-specific argument names to
     * appear in our error messages.
     */
    private static String massageLangchain4jError(ContextSupport context, String message) {
        if (message.contains("maxChunkSize")) {
            String optionName = context.getOptionNameForMessage(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE);
            message = message.replace("maxChunkSize", optionName);
        }
        if (message.contains("maxOverlapSize")) {
            String optionName = context.getOptionNameForMessage(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE);
            message = message.replace("maxOverlapSize", optionName);
        }
        return message;
    }

    private DocumentSplitterFactory() {
    }
}
