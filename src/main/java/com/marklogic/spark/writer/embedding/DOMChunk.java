/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.spark.ConnectorException;
import dev.langchain4j.data.embedding.Embedding;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

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
    public void addEmbedding(Embedding embedding) {
        // DOM is fine with null as a value for the namespace.
        Element embeddingElement = document.createElementNS(xmlChunkConfig.getEmbeddingNamespace(), xmlChunkConfig.getEmbeddingName());
        embeddingElement.setTextContent(embedding.vectorAsList().toString());
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
