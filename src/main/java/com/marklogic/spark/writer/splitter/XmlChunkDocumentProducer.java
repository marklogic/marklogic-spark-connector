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

import java.util.List;

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
    }

    @Override
    protected DocumentWriteOperation makeChunkDocument() {
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

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "xml");
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JDOMHandle(doc));
    }

    protected DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());

        final Element chunks = new Element("chunks");
        doc.getRootElement().addContent(chunks);
        for (TextSegment textSegment : textSegments) {
            chunks.addContent(new Element("chunk")
                .addContent(new Element("text")
                    .addContent(textSegment.text())));
        }

        return new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JDOMHandle(doc));
    }

    private Element newElement(String name) {
        String ns = chunkConfig.getXmlNamespace();
        return ns != null ? new Element(name, ns) : new Element(name);
    }
}
