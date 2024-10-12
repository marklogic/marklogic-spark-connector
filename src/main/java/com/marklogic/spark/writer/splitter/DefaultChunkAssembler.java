/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.*;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.Util;
import com.marklogic.spark.writer.JsonUtil;
import com.marklogic.spark.writer.XmlUtil;
import dev.langchain4j.data.segment.TextSegment;
import org.jdom2.Document;
import org.jdom2.Element;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class DefaultChunkAssembler implements ChunkAssembler {

    private static final String CHUNKS_ARRAY = "chunks";

    private final DocumentMetadataHandle chunkMetadata;
    private final int maxChunksPerDocument;

    public DefaultChunkAssembler(DocumentMetadataHandle chunkMetadata, int maxChunksPerDocument) {
        this.chunkMetadata = chunkMetadata;
        this.maxChunksPerDocument = maxChunksPerDocument;
    }

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        final Format format = determineSourceDocumentFormat(sourceDocument);
        if (format == null) {
            Util.MAIN_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        if (Format.TEXT.equals(format)) {
            return writeChunksToSeparateDocument(sourceDocument, textSegments);
        }

        if (Format.JSON.equals(format)) {
            if (maxChunksPerDocument > 0) {
                return new JsonChunkDocumentProducer(sourceDocument, textSegments, chunkMetadata, maxChunksPerDocument);
            }
            return addChunksToJsonDocument(sourceDocument, textSegments);
        }

        // Default to XML for now. We'll add a config option for this soon.
        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());
        addChunksToXmlDocument(doc, textSegments);
        DocumentWriteOperation output = new DocumentWriteOperationImpl(sourceDocument.getUri(),
            sourceDocument.getMetadata(), new JDOMHandle(doc));
        return Stream.of(output).iterator();
    }

    private Format determineSourceDocumentFormat(DocumentWriteOperation sourceDocument) {
        final AbstractWriteHandle content = sourceDocument.getContent();
        final String uri = sourceDocument.getUri() != null ? sourceDocument.getUri() : "";
        if (content instanceof JacksonHandle || uri.endsWith(".json")) {
            return Format.JSON;
        }
        if (content instanceof DOMHandle || content instanceof JDOMHandle || uri.endsWith(".xml")) {
            return Format.XML;
        }
        if (content instanceof BaseHandle) {
            return ((BaseHandle) content).getFormat();
        }
        return null;
    }

    private Iterator<DocumentWriteOperation> writeChunksToSeparateDocument(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        Document doc = new Document();
        Element root = new Element("root");
        doc.addContent(root);
        root.addContent(new Element("source-uri").addContent(sourceDocument.getUri()));
        addChunksToXmlDocument(doc, textSegments);

        // Temporary URI and permissions, will make these nicer later.
        String uri = sourceDocument.getUri() + "-chunks.xml";
        return Stream.of(
            sourceDocument,
            new DocumentWriteOperationImpl(uri, this.chunkMetadata, new JDOMHandle(doc))
        ).iterator();
    }

    private void addChunksToXmlDocument(Document doc, List<TextSegment> textSegments) {
        final Element chunks = new Element(CHUNKS_ARRAY);
        doc.getRootElement().addContent(chunks);
        textSegments.forEach(textSegment -> chunks
            .addContent(new Element("chunk").addContent(new Element("text").addContent(textSegment.text())))
        );
    }

    private Iterator<DocumentWriteOperation> addChunksToJsonDocument(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) JsonUtil.getJsonFromHandle(content);

        ArrayNode chunks = doc.putArray(CHUNKS_ARRAY);
        textSegments.forEach(textSegment -> chunks.addObject().put("text", textSegment.text()));

        DocumentWriteOperation result = new DocumentWriteOperationImpl(sourceDocument.getUri(),
            sourceDocument.getMetadata(), new JacksonHandle(doc));

        return Stream.of(result).iterator();
    }

    // This will likely end up in its own class file, just stashing it here for now.
    private static class JsonChunkDocumentProducer implements Iterator<DocumentWriteOperation> {
        private final DocumentWriteOperation sourceDocument;
        private final List<TextSegment> textSegments;
        private final DocumentMetadataHandle chunkMetadata;
        private final int maxChunks;
        private final ObjectMapper objectMapper = new ObjectMapper();

        private int listIndex = -1;
        private int counter;

        JsonChunkDocumentProducer(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments, DocumentMetadataHandle chunkMetadata, int maxChunks) {
            this.sourceDocument = sourceDocument;
            this.textSegments = textSegments;
            this.chunkMetadata = chunkMetadata;
            this.maxChunks = maxChunks;
        }

        @Override
        public boolean hasNext() {
            return listIndex < textSegments.size();
        }

        // Sonar complains that a NoSuchElementException should be thrown here, but that would only occur if the
        // hasNext() implementation has a bug, not if the user calls this too many times.
        @SuppressWarnings("java:S2272")
        @Override
        public DocumentWriteOperation next() {
            if (listIndex == -1) {
                listIndex++;
                return sourceDocument;
            }
            String uri = sourceDocument.getUri() + "-chunk-" + counter++ + ".json";
            ObjectNode doc = objectMapper.createObjectNode();
            doc.put("source-uri", sourceDocument.getUri());
            ArrayNode chunks = doc.putArray(CHUNKS_ARRAY);
            for (int i = 0; i < maxChunks && hasNext(); i++) {
                chunks.addObject().put("text", textSegments.get(listIndex++).text());
            }
            return new DocumentWriteOperationImpl(uri, chunkMetadata, new JacksonHandle(doc));
        }
    }
}
