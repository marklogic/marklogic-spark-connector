/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.spark.writer.XmlUtil;
import dev.langchain4j.data.segment.TextSegment;
import org.jdom2.Document;
import org.jdom2.Element;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

class XmlChunkDocumentProducer implements Iterator<DocumentWriteOperation> {

    private final DocumentWriteOperation sourceDocument;
    private final List<TextSegment> textSegments;
    private final ChunkConfig chunkConfig;
    private final int maxChunksPerDocument;

    private int listIndex = -1;
    private int counter;

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        this.sourceDocument = sourceDocument;
        this.textSegments = textSegments;
        this.chunkConfig = chunkConfig;

        // Chunks cannot be written to a TEXT document. So if maxChunks is zero, and we have a text document, we will
        // instead write all the chunks to a separate document.
        this.maxChunksPerDocument = Format.TEXT.equals(sourceDocumentFormat) && chunkConfig.getMaxChunks() == 0 ?
            textSegments.size() :
            chunkConfig.getMaxChunks();
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
            if (this.maxChunksPerDocument == 0) {
                listIndex = textSegments.size();
                return addChunksToSourceDocument();
            }
            listIndex++;
            return sourceDocument;
        }

        Document doc = new Document();
        Element root = newElement(chunkConfig.getRootName() != null ? chunkConfig.getRootName() : "root");
        doc.addContent(root);
        root.addContent(newElement("source-uri").addContent(sourceDocument.getUri()));
        final Element chunks = newElement("chunks");
        doc.getRootElement().addContent(chunks);

        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            chunks.addContent(newElement("chunk")
                .addContent(newElement("text")
                    .addContent(textSegments.get(listIndex++).text())));
        }

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument);
        counter++;
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JDOMHandle(doc));
    }

    private DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());
        addChunksToXmlDocument(doc, textSegments);
        return new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JDOMHandle(doc));
    }

    private void addChunksToXmlDocument(Document doc, List<TextSegment> textSegments) {
        final Element chunks = new Element("chunks");
        doc.getRootElement().addContent(chunks);
        for (TextSegment textSegment : textSegments) {
            chunks.addContent(new Element("chunk")
                .addContent(new Element("text")
                    .addContent(textSegment.text())));
        }
    }

    private Element newElement(String name) {
        String ns = chunkConfig.getXmlNamespace();
        return ns != null ? new Element(name, ns) : new Element(name);
    }

    private String makeChunkDocumentUri(DocumentWriteOperation sourceDocument) {
        if (chunkConfig.getUriPrefix() == null && chunkConfig.getUriSuffix() == null) {
            return String.format("%s-chunks-%d.xml", sourceDocument.getUri(), counter);
        }

        String uri = UUID.randomUUID().toString();
        if (chunkConfig.getUriPrefix() != null) {
            uri = chunkConfig.getUriPrefix() + uri;
        }
        if (chunkConfig.getUriSuffix() != null) {
            uri += chunkConfig.getUriSuffix();
        }
        return uri;
    }
}
