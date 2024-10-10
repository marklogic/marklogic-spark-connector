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
        if (context.hasOption(Options.WRITE_SPLITTER_REGEX)) {
            return makeRegexSplitter(context);
        }
        return makeDefaultSplitter(context);
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
        return (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000, 0);
    }

    private static int getMaxOverlapSize(ContextSupport context) {
        return (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0, 0);
    }

    private DocumentSplitterFactory() {
    }
}
