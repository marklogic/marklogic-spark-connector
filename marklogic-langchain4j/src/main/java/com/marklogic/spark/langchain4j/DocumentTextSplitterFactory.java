/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.langchain4j.splitter.DocumentSplitterFactory;
import com.marklogic.langchain4j.splitter.DocumentTextSplitter;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.splitter.*;
import com.marklogic.spark.dom.NamespaceContextFactory;
import dev.langchain4j.data.document.DocumentSplitter;

import java.util.Arrays;
import java.util.Optional;

public interface DocumentTextSplitterFactory {

    static Optional<DocumentTextSplitter> makeSplitter(Context context) {
        if (context.hasOption(Options.WRITE_SPLITTER_XPATH)) {
            return Optional.of(makeXmlSplitter(context));
        } else if (context.getProperties().containsKey(Options.WRITE_SPLITTER_JSON_POINTERS)) {
            // "" is a valid JSON Pointer expression, so we only check to see if the key exists.
            return Optional.of(makeJsonSplitter(context));
        } else if (context.getBooleanOption(Options.WRITE_SPLITTER_TEXT, false)) {
            return Optional.of(makeTextSplitter(context));
        }
        return Optional.empty();
    }

    private static DocumentTextSplitter makeXmlSplitter(Context context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split XML documents using XPath: {}",
                context.getStringOption(Options.WRITE_SPLITTER_XPATH));
        }
        TextSelector textSelector = makeXmlTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        ChunkAssembler chunkAssembler = ChunkAssemblerFactory.makeChunkAssembler(context);
        return new DocumentTextSplitter(textSelector, splitter, chunkAssembler);
    }

    static TextSelector makeXmlTextSelector(Context context) {
        return makeXmlTextSelector(context.getStringOption(Options.WRITE_SPLITTER_XPATH), context);
    }

    static TextSelector makeXmlTextSelector(String xpath, Context context) {
        return new DOMTextSelector(xpath, NamespaceContextFactory.makeNamespaceContext(context.getProperties()));
    }

    private static DocumentTextSplitter makeJsonSplitter(Context context) {
        TextSelector textSelector = makeJsonTextSelector(context.getProperties().get(Options.WRITE_SPLITTER_JSON_POINTERS));
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        return new DocumentTextSplitter(textSelector, splitter, ChunkAssemblerFactory.makeChunkAssembler(context));
    }

    static TextSelector makeJsonTextSelector(String jsonPointers) {
        String[] pointers = jsonPointers.split("\n");
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split JSON documents using JSON Pointers: {}", Arrays.asList(pointers));
        }
        // Need an option other than "join delimiter", which applies to joining split text, not selected text.
        return new JsonPointerTextSelector(pointers, null);
    }

    private static DocumentTextSplitter makeTextSplitter(Context context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split text documents using all text in each document.");
        }
        return new DocumentTextSplitter(new AllTextSelector(),
            DocumentSplitterFactory.makeDocumentSplitter(context), ChunkAssemblerFactory.makeChunkAssembler(context)
        );
    }
}
