/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class DOMChunkSelector implements ChunkSelector {

    private final XPathFactory xpathFactory;
    private final XPathExpression chunksExpression;
    private final String chunkTextExpression;
    private final DocumentBuilderFactory documentBuilderFactory;

    public DOMChunkSelector(String chunksExpression, String chunkTextExpression) {
        this.xpathFactory = XPathFactory.newInstance();
        try {
            this.chunksExpression = this.xpathFactory.newXPath().compile(chunksExpression);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format(
                "Unable to compile XPath expression for selecting chunks: %s; cause: %s", chunksExpression, e.getMessage()), e);
        }
        this.chunkTextExpression = chunkTextExpression;
        this.documentBuilderFactory = DocumentBuilderFactory.newInstance();
    }

    @Override
    public DocumentAndChunks selectChunks(DocumentWriteOperation sourceDocument) {
        Document doc = extractDocument(sourceDocument);

        NodeList chunkNodes = selectChunkNodes(doc);
        if (chunkNodes.getLength() == 0) {
            return new DocumentAndChunks(sourceDocument, null);
        }

        List<Chunk> chunks = makeChunks(sourceDocument, doc, chunkNodes);
        DocumentWriteOperation docToWrite = new DocumentWriteOperationImpl(sourceDocument.getUri(),
            sourceDocument.getMetadata(), new DOMHandle(doc));
        return new DocumentAndChunks(docToWrite, chunks);
    }

    private Document extractDocument(DocumentWriteOperation sourceDocument) {
        AbstractWriteHandle handle = sourceDocument.getContent();
        if (handle instanceof DOMHandle) {
            return ((DOMHandle) handle).get();
        }
        String xml = HandleAccessor.contentAsString(handle);
        try {
            return documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to parse XML for document with URI: %s; cause: %s",
                sourceDocument.getUri(), e.getMessage()), e);
        }
    }

    private NodeList selectChunkNodes(Document doc) {
        try {
            return (NodeList) chunksExpression.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format(
                "Unable to evaluate XPath expression for selecting chunks: %s; cause: %s", chunksExpression, e.getMessage()), e);
        }
    }

    private List<Chunk> makeChunks(DocumentWriteOperation sourceDocument, Document document, NodeList chunkNodes) {
        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < chunkNodes.getLength(); i++) {
            Node node = chunkNodes.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) {
                throw new ConnectorException(String.format("XPath expression for selecting chunks must only " +
                    "select elements; XPath: %s; document URI: %s", chunksExpression, sourceDocument.getUri()));
            }
            chunks.add(new DOMChunk(sourceDocument.getUri(), document, (Element) node, chunkTextExpression, xpathFactory));
        }
        return chunks;
    }
}
