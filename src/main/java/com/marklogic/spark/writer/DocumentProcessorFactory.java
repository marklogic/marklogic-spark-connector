/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.splitter.*;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import org.jdom2.Namespace;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Only supports building a {@code SplitterDocumentProcessor}, but may later support custom processors as well.
 */
public abstract class DocumentProcessorFactory {

    public static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        if (context.hasOption(Options.WRITE_SPLITTER_XML_PATH)) {
            TextSelector textSelector = makeTextSelector(context);
            DocumentSplitter splitter = makeDefaultSplitter(context);
            ChunkAssembler chunkAssembler = makeChunkProcessor();
            return new SplitterDocumentProcessor(textSelector, splitter, chunkAssembler);
        }
        return null;
    }

    private static TextSelector makeTextSelector(ContextSupport context) {
        String path = context.getStringOption(Options.WRITE_SPLITTER_XML_PATH);
        List<Namespace> namespaces = context.getProperties().keySet()
            .stream()
            .filter(key -> key.startsWith(Options.WRITE_SPLITTER_XML_NAMESPACE_PREFIX))
            .map(key -> {
                String prefix = key.substring(Options.WRITE_SPLITTER_XML_NAMESPACE_PREFIX.length());
                return Namespace.getNamespace(prefix, context.getStringOption(key));
            })
            .collect(Collectors.toList());
        return new JDOMTextSelector(path, namespaces);
    }

    private static ChunkAssembler makeChunkProcessor() {
        return new DefaultChunkAssembler();
    }

    private static DocumentSplitter makeDefaultSplitter(ContextSupport context) {
        int maxChunkSize = (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000, 0);
        int maxOverlapSize = (int) context.getNumericOption(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0, 0);
        return DocumentSplitters.recursive(maxChunkSize, maxOverlapSize);
    }

    private DocumentProcessorFactory() {
    }
}
