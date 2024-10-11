/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

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

    @Override
    public Iterator<DocumentWriteOperation> assembleChunks(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        Format format = determineSourceDocumentFormat(sourceDocument);
        if (format == null) {
            Util.MAIN_LOGGER.warn("Cannot split document with URI {}; cannot determine the document format.", sourceDocument.getUri());
            return Stream.of(sourceDocument).iterator();
        }

        if (Format.TEXT.equals(format)) {
            return writeChunksToSeparateDocument(sourceDocument, textSegments);
        }

        if (Format.JSON.equals(format)) {
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
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.getPermissions().addFromDelimitedString("spark-user-role,read,spark-user-role,update");
        return Stream.of(
            sourceDocument,
            new DocumentWriteOperationImpl(uri, metadata, new JDOMHandle(doc))
        ).iterator();
    }

    private void addChunksToXmlDocument(Document doc, List<TextSegment> textSegments) {
        final Element chunks = new Element("chunks");
        doc.getRootElement().addContent(chunks);
        textSegments.forEach(textSegment -> chunks
            .addContent(new Element("chunk").addContent(new Element("text").addContent(textSegment.text())))
        );
    }

    private Iterator<DocumentWriteOperation> addChunksToJsonDocument(DocumentWriteOperation sourceDocument, List<TextSegment> textSegments) {
        AbstractWriteHandle content = sourceDocument.getContent();
        ObjectNode doc = (ObjectNode) JsonUtil.getJsonFromHandle(content);

        ArrayNode chunks = doc.putArray("chunks");
        textSegments.forEach(textSegment -> chunks.addObject().put("text", textSegment.text()));

        DocumentWriteOperation result = new DocumentWriteOperationImpl(sourceDocument.getUri(),
            sourceDocument.getMetadata(), new JacksonHandle(doc));

        return Stream.of(result).iterator();
    }
}
