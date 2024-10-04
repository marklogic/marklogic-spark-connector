/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.splitter.*;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;

import java.util.HashMap;
import java.util.Map;

public abstract class DocumentProcessorFactory {

    public static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        if (context.hasOption(Options.WRITE_SPLITTER_JSON_POINTERS)) {
            TextSelector textSelector = makeTextSelector(context);
            DocumentSplitter splitter = context.hasOption(Options.WRITE_SPLITTER_CUSTOM_CLASS) ?
                makeCustomSplitter(context) :
                makeDefaultSplitter(context);
            ChunkProcessor chunkProcessor = makeChunkProcessor(context);
            return new SplitterDocumentProcessor(textSelector, splitter, chunkProcessor);
        }
        return null;
    }

    private static TextSelector makeTextSelector(ContextSupport context) {
        String jsonPointer = context.getStringOption(Options.WRITE_SPLITTER_JSON_POINTERS);
        return new JsonTextSelector(jsonPointer);
    }

    private static ChunkProcessor makeChunkProcessor(ContextSupport context) {
        if (context.hasOption(Options.WRITE_SPLITTER_OUTPUT)) {
            String output = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT);
            if ("sidecar".equalsIgnoreCase(output)) {
                return new SidecarDocumentChunkProcessor(context);
            }
            return new ChunkDocumentChunkProcessor(context);
        }
        return new SourceDocumentChunkProcessor();
    }

    private static DocumentSplitter makeDefaultSplitter(ContextSupport context) {
        int maxChunkSize = (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000, 0);
        int maxOverlapSize = (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0, 0);
        return DocumentSplitters.recursive(maxChunkSize, maxOverlapSize);
    }

    private static DocumentSplitter makeCustomSplitter(ContextSupport context) {
        String customClass = context.getStringOption(Options.WRITE_SPLITTER_CUSTOM_CLASS);
        Map<String, String> customClassOptions = makeCustomSplitterOptions(context);
        try {
            return (DocumentSplitter) Class.forName(customClass)
                .getDeclaredConstructor(Map.class)
                .newInstance(customClassOptions);
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to create custom splitter with class name: %s; cause: %s", customClass, e.getMessage()), e);
        }
    }

    private static Map<String, String> makeCustomSplitterOptions(ContextSupport context) {
        Map<String, String> options = new HashMap<>();
        context.getProperties().forEach((key, value) -> {
            if (key.startsWith(Options.WRITE_SPLITTER_CUSTOM_OPTION_PREFIX)) {
                String name = key.substring(Options.WRITE_SPLITTER_CUSTOM_OPTION_PREFIX.length());
                options.put(name, value);
            }
        });
        return options;
    }
}
