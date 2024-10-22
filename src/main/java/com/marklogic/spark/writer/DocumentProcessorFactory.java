/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.writer.embedding.EmbedderDocumentProcessor;
import com.marklogic.spark.writer.splitter.*;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.jdom2.Namespace;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Only supports building a {@code SplitterDocumentProcessor}, but may later support custom processors as well.
 * <p>
 * So maybe what we need here is the ability for Options to define the FQN of additional document processor classes.
 * All the Embedder stuff would then go into a separate artifact. That artifact could be safely included in Flux,
 * as while it would require Java 17, it wouldn't be accessed unless the user provides embedding options on the CLI.
 * Flux would then add an option with the FQN of the DocumentProcessor class that implements embedding.
 * <p>
 * So we need an option we look here that defines a factory - so:
 * Function<DocumentProcessor, Properties/>
 */
public abstract class DocumentProcessorFactory {

    public static DocumentProcessor buildDocumentProcessor(ContextSupport context) {
        List<Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>> processors = new ArrayList<>();
        DocumentProcessor splitterProcessor = makeSplitter(context);
        if (splitterProcessor != null) {
            processors.add(splitterProcessor);
        }

        if (context.hasOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME)) {
            String className = context.getStringOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME);
            Map<String, String> options = new HashMap<>();
            context.getProperties().keySet().stream()
                .filter(key -> key.startsWith(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX))
                .forEach(key -> options.put(key.substring(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX.length()), context.getProperties().get(key)));

            try {
                Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
                EmbeddingModel embeddingModel = ((Function<Map<String, String>, EmbeddingModel>) instance).apply(options);
                int batchSize = context.getIntOption(Options.WRITE_EMBEDDER_BATCH_SIZE, 1, 1);
                processors.add(new EmbedderDocumentProcessor(embeddingModel, batchSize));
            } catch (Exception ex) {
                throw new ConnectorException("oops", ex);
            }
        }
//        if (context.hasOption(Options.WRITE_DOCUMENT_PROCESSOR_FACTORY)) {
//            /**
//             * Is this over-engineered? Perhaps we just need the ability to accept a custom document processor, which
//             * could of course include multiple capabilities in it if a user makes their own?
//             */
//            String className = context.getStringOption(Options.WRITE_DOCUMENT_PROCESSOR_FACTORY);
//            Util.MAIN_LOGGER.warn("CLASS: " + className);
//            try {
//                Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
//                Function<Map<String, String>, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>> factory =
//                    (Function<Map<String, String>, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>>) instance;
//                processors.add(factory.apply(context.getProperties()));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
        if (processors.isEmpty()) {
            return null;
        }
        return new CompositeDocumentProcessor(processors);
    }

    private static DocumentProcessor makeSplitter(ContextSupport context) {
        if (context.hasOption(Options.WRITE_SPLITTER_XML_PATH)) {
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
                context.getStringOption(Options.WRITE_SPLITTER_XML_PATH));
        }
        TextSelector textSelector = makeXmlTextSelector(context);
        DocumentSplitter splitter = DocumentSplitterFactory.makeDocumentSplitter(context);
        ChunkAssembler chunkAssembler = makeChunkAssembler(context);
        return new SplitterDocumentProcessor(textSelector, splitter, chunkAssembler);
    }

    private static TextSelector makeXmlTextSelector(ContextSupport context) {
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
        if (context.hasOption(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS)) {
            metadata.getCollections().addAll(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS).split(","));
        }
        if (context.hasOption(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS);
            metadata.getPermissions().addFromDelimitedString(value);
        } else if (context.hasOption(Options.WRITE_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_PERMISSIONS);
            metadata.getPermissions().addFromDelimitedString(value);
        }

        return new DefaultChunkAssembler(new ChunkConfig.Builder()
            .withMetadata(metadata)
            .withMaxChunks(context.getIntOption(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 0, 0))
            .withDocumentType(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_DOCUMENT_TYPE))
            .withRootName(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_ROOT_NAME))
            .withUriPrefix(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_URI_PREFIX))
            .withUriSuffix(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_URI_SUFFIX))
            .withXmlNamespace(context.getStringOption(Options.WRITE_SPLITTER_OUTPUT_XML_NAMESPACE))
            .build());
    }

    private DocumentProcessorFactory() {
    }

    private static class CompositeDocumentProcessor implements DocumentProcessor {

        private List<Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>> processors;

        private CompositeDocumentProcessor(List<Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>> processors) {
            this.processors = processors;
        }

        @Override
        public Iterator<DocumentWriteOperation> apply(DocumentWriteOperation sourceDocument) {
            Iterator<DocumentWriteOperation> outputIterator = Stream.of(sourceDocument).iterator();
            // Could just process everything into a list for now...
            for (int i = 0; i < processors.size(); i++) {
                List<DocumentWriteOperation> outputDocuments = new ArrayList<>();
                while (outputIterator.hasNext()) {
                    Iterator<DocumentWriteOperation> currentIterator = processors.get(i).apply(outputIterator.next());
                    while (currentIterator.hasNext()) {
                        outputDocuments.add(currentIterator.next());
                    }
                }
                outputIterator = outputDocuments.iterator();
            }
            return outputIterator;
        }
    }
}
