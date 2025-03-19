/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import dev.langchain4j.data.document.DefaultDocument;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentByRegexSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;

import java.util.HashMap;
import java.util.Map;

public interface DocumentSplitterFactory {

    static DocumentSplitter makeDocumentSplitter(Context context) {
        if (context.hasOption(Options.WRITE_SPLITTER_CUSTOM_CLASS)) {
            return makeCustomSplitter(context);
        }

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

    private static DocumentSplitter makeCustomSplitter(Context context) {
        String className = context.getStringOption(Options.WRITE_SPLITTER_CUSTOM_CLASS);
        Map<String, String> options = new HashMap<>();
        context.getProperties().keySet().stream()
            .filter(key -> key.startsWith(Options.WRITE_SPLITTER_CUSTOM_CLASS_OPTION_PREFIX))
            .forEach(key -> options.put(key.substring(Options.WRITE_SPLITTER_CUSTOM_CLASS_OPTION_PREFIX.length()), context.getStringOption(key)));
        try {
            return (DocumentSplitter) Class.forName(className).getDeclaredConstructor(Map.class).newInstance(options);
        } catch (ClassNotFoundException ex) {
            throw new ConnectorException(String.format("Cannot find custom splitter with class name: %s", className), ex);
        } catch (NoSuchMethodException ex) {
            throw new ConnectorException(String.format("Cannot create custom splitter with class name: %s; " +
                "the class must have a public constructor that accepts a java.util.Map<String, String>.", className), ex);
        } catch (Exception ex) {
            throw new ConnectorException(String.format("Unable to instantiate custom splitter with class name " +
                "%s; cause: %s", className, ex.getMessage()), ex);
        }
    }

    private static DocumentSplitter makeDefaultSplitter(Context context) {
        return DocumentSplitters.recursive(
            getMaxChunkSize(context),
            getMaxOverlapSize(context)
        );
    }

    private static DocumentSplitter makeRegexSplitter(Context context) {
        String regex = context.getStringOption(Options.WRITE_SPLITTER_REGEX);
        String joinDelimiter = context.getStringOption(Options.WRITE_SPLITTER_JOIN_DELIMITER, " ");
        DocumentSplitter splitter = new DocumentByRegexSplitter(regex, joinDelimiter, getMaxChunkSize(context), getMaxOverlapSize(context));
        // Test the splitter to ensure an invalid regex causes an immediate failure.
        try {
            splitter.split(new DefaultDocument("Test data"));
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Cannot split documents due to invalid regex: %s; cause: %s", regex, e.getMessage()), e);
        }
        return splitter;
    }

    private static int getMaxChunkSize(Context context) {
        return context.getIntOption(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000, 0);
    }

    private static int getMaxOverlapSize(Context context) {
        return context.getIntOption(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0, 0);
    }

    /**
     * langchain4j does a nice job with validating inputs, but we don't want langchain4j-specific argument names to
     * appear in our error messages.
     */
    private static String massageLangchain4jError(Context context, String message) {
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
}
