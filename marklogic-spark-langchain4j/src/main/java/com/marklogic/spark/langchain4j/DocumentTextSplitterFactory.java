/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.langchain4j.splitter.*;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import dev.langchain4j.data.document.DocumentSplitter;

import java.util.Arrays;
import java.util.Optional;

public abstract class DocumentTextSplitterFactory {

    public static Optional<DocumentTextSplitter> makeSplitter(Context context) {
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
        ChunkAssembler chunkAssembler = makeChunkAssembler(context);
        return new DocumentTextSplitter(textSelector, splitter, chunkAssembler);
    }

    private static TextSelector makeXmlTextSelector(Context context) {
        String xpath = context.getStringOption(Options.WRITE_SPLITTER_XPATH);
        return new DOMTextSelector(xpath, NamespaceContextFactory.makeNamespaceContext(context.getProperties()));
    }

    private static DocumentTextSplitter makeJsonSplitter(Context context) {
        TextSelector textSelector = makeJsonTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        return new DocumentTextSplitter(textSelector, splitter, makeChunkAssembler(context));
    }

    private static TextSelector makeJsonTextSelector(Context context) {
        String value = context.getProperties().get(Options.WRITE_SPLITTER_JSON_POINTERS);
        String[] pointers = value.split("\n");
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
            DocumentSplitterFactory.makeDocumentSplitter(context), makeChunkAssembler(context)
        );
    }

    private static ChunkAssembler makeChunkAssembler(Context context) {
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
            .withXmlNamespace(context.getProperties().get(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE))
            .withEmbeddingXmlNamespace(context.getProperties().get(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE))
            .build()
        );
    }

    private DocumentTextSplitterFactory() {
    }
}
