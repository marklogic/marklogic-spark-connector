/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.writer.splitter.*;
import dev.langchain4j.data.document.DocumentSplitter;
import org.jdom2.Namespace;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Only supports building a {@code SplitterDocumentProcessor}, but may later support custom processors as well.
 */
public abstract class DocumentProcessorFactory {

    public static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        if (context.hasOption(Options.WRITE_SPLITTER_XPATH)) {
            return makeXmlSplitter(context);
        } else if (context.getProperties().containsKey(Options.WRITE_SPLITTER_JSON_POINTERS)) {
            // "" is a valid JSON Pointer expression, so we only check to see if the key exists.
            return makeJsonSplitter(context);
        } else if (context.getBooleanOption(Options.WRITE_SPLITTER_TEXT, false)) {
            return makeTextSplitter(context);
        }
        return null;
    }

    private static SplitterDocumentProcessor makeXmlSplitter(ContextSupport context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split XML documents using XPath: {}",
                context.getStringOption(Options.WRITE_SPLITTER_XPATH));
        }
        TextSelector textSelector = makeXmlTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        ChunkAssembler chunkAssembler = makeChunkAssembler(context);
        return new SplitterDocumentProcessor(textSelector, splitter, chunkAssembler);
    }

    private static TextSelector makeXmlTextSelector(ContextSupport context) {
        String xpath = context.getStringOption(Options.WRITE_SPLITTER_XPATH);
        List<Namespace> namespaces = context.getProperties().keySet()
            .stream()
            .filter(key -> key.startsWith(Options.XPATH_NAMESPACE_PREFIX))
            .map(key -> {
                String prefix = key.substring(Options.XPATH_NAMESPACE_PREFIX.length());
                return Namespace.getNamespace(prefix, context.getStringOption(key));
            })
            .collect(Collectors.toList());
        return new JDOMTextSelector(xpath, namespaces);
    }

    private static SplitterDocumentProcessor makeJsonSplitter(ContextSupport context) {
        TextSelector textSelector = makeJsonTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        return new SplitterDocumentProcessor(textSelector, splitter, makeChunkAssembler(context));
    }

    private static TextSelector makeJsonTextSelector(ContextSupport context) {
        String value = context.getProperties().get(Options.WRITE_SPLITTER_JSON_POINTERS);
        String[] pointers = value.split("\n");
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split JSON documents using JSON Pointers: {}", Arrays.asList(pointers));
        }
        // Need an option other than "join delimiter", which applies to joining split text, not selected text.
        return new JsonPointerTextSelector(pointers, null);
    }

    private static SplitterDocumentProcessor makeTextSplitter(ContextSupport context) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will split text documents using all text in each document.");
        }
        return new SplitterDocumentProcessor(new AllTextSelector(),
            DocumentSplitterFactory.makeDocumentSplitter(context), makeChunkAssembler(context)
        );
    }

    private static ChunkAssembler makeChunkAssembler(ContextSupport context) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (context.hasOption(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS)) {
            metadata.getCollections().addAll(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS).split(","));
        }
        if (context.hasOption(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS);
            metadata.getPermissions().addFromDelimitedString(value);
        } else if (context.hasOption(Options.WRITE_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_PERMISSIONS);
            metadata.getPermissions().addFromDelimitedString(value);
        }

        return new DefaultChunkAssembler(new ChunkConfig.Builder()
            .withMetadata(metadata)
            .withMaxChunks(context.getIntOption(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 0, 0))
            .withDocumentType(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE))
            .withRootName(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME))
            .withUriPrefix(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_URI_PREFIX))
            .withUriSuffix(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_URI_SUFFIX))
            .withXmlNamespace(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE))
            .build());
    }

    private DocumentProcessorFactory() {
    }
}
