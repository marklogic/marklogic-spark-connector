/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.spark.ConnectorException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.List;

public class DOMChunk implements Chunk {

    private final String documentUri;
    private final Document document;
    private final Element chunkElement;
    private final XmlChunkConfig xmlChunkConfig;
    private final XPathFactory xpathFactory;

    public DOMChunk(String documentUri, Document document, Element chunkElement, XmlChunkConfig xmlChunkConfig, XPathFactory xpathFactory) {
        this.documentUri = documentUri;
        this.document = document;
        this.chunkElement = chunkElement;
        this.xmlChunkConfig = xmlChunkConfig;
        this.xpathFactory = xpathFactory;
    }

    @Override
    public String getDocumentUri() {
        return documentUri;
    }

    public boolean hasEmbeddingText() {
        String text = getEmbeddingText();
        return text != null && !text.trim().isEmpty();
    }

    @Override
    public String getEmbeddingText() {
        NodeList embeddingTextNodes;
        String textExpression = xmlChunkConfig.getTextExpression();

        XPath xpath = xpathFactory.newXPath();
        if (xmlChunkConfig.getNamespaceContext() != null) {
            xpath.setNamespaceContext(xmlChunkConfig.getNamespaceContext());
        }

        try {
            embeddingTextNodes = (NodeList) xpath.evaluate(textExpression, chunkElement, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format("Unable to evaluate XPath expression: %s; cause: %s",
                textExpression, e.getMessage()), e);
        }

        return concatenateNodesIntoString(embeddingTextNodes);
    }

    @Override
    public void addEmbedding(float[] embedding) {
        // DOM is fine with null as a value for the namespace.
        Element embeddingElement = document.createElementNS(xmlChunkConfig.getEmbeddingNamespace(), xmlChunkConfig.getEmbeddingName());
        List<Float> values = new ArrayList<>(embedding.length);
        for (float val : embedding) {
            values.add(val);
        }
        embeddingElement.setTextContent(values.toString());
        chunkElement.appendChild(embeddingElement);
    }

    private String concatenateNodesIntoString(NodeList embeddingTextNodes) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < embeddingTextNodes.getLength(); i++) {
            if (i > 0) {
                builder.append(" ");
            }
            builder.append(embeddingTextNodes.item(i).getTextContent());
        }
        return builder.toString().trim();
    }
}
