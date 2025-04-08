/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.dom.DOMHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.List;

public class DOMChunkSelector implements ChunkSelector {

    private final XPathFactory xpathFactory;
    private final XPathExpression chunksExpression;
    private final XmlChunkConfig xmlChunkConfig;
    private final DOMHelper domHelper;

    public DOMChunkSelector(String chunksExpression, XmlChunkConfig xmlChunkConfig) {
        this.xpathFactory = XPathFactory.newInstance();
        this.xmlChunkConfig = xmlChunkConfig;
        this.domHelper = new DOMHelper(xmlChunkConfig.getNamespaceContext());

        String chunksXPath = chunksExpression != null ? chunksExpression : "/node()/chunks";
        this.chunksExpression = domHelper.compileXPath(chunksXPath, "selecting chunks");
    }

    @Override
    public DocumentAndChunks selectChunks(String uri, AbstractWriteHandle content) {
        Document doc = domHelper.extractDocument(content, uri);
        NodeList chunkNodes = selectChunkNodes(doc);
        if (chunkNodes.getLength() == 0) {
            return null;
        }
        List<Chunk> chunks = makeChunks(uri, doc, chunkNodes);
        return new DocumentAndChunks(new DocumentWriteOperationImpl(uri, null, new DOMHandle(doc)), chunks);
    }

    private NodeList selectChunkNodes(Document doc) {
        try {
            return (NodeList) chunksExpression.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format(
                "Unable to evaluate XPath expression for selecting chunks: %s; cause: %s", chunksExpression, e.getMessage()), e);
        }
    }

    private List<Chunk> makeChunks(String sourceUri, Document document, NodeList chunkNodes) {
        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < chunkNodes.getLength(); i++) {
            Node node = chunkNodes.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) {
                throw new ConnectorException(String.format("XPath expression for selecting chunks must only " +
                    "select elements; XPath: %s; document URI: %s", chunksExpression, sourceUri));
            }
            DOMChunk chunk = new DOMChunk(document, (Element) node, xmlChunkConfig, xpathFactory);
            if (chunk.hasEmbeddingText()) {
                chunks.add(chunk);
            }
        }
        return chunks;
    }
}
