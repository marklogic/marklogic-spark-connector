/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.dom.DOMHelper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

public class DOMTextSelector implements TextSelector {

    private static final String JOIN_DELIMITER = " ";

    private final DOMHelper domHelper;
    private final XPathExpression textExpression;

    public DOMTextSelector(String textExpression, NamespaceContext namespaceContext) {
        this.domHelper = new DOMHelper(namespaceContext);
        this.textExpression = domHelper.compileXPath(textExpression, "selecting text");
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation sourceDocument) {
        Document doc;
        try {
            doc = domHelper.extractDocument(sourceDocument);
        } catch (Exception ex) {
            Util.MAIN_LOGGER.warn("Unable to select text to split in document: {}; cause: {}", sourceDocument.getUri(), ex.getMessage());
            return null;
        }
        return selectTextInDocument(doc);
    }

    @Override
    public String selectTextToSplit(AbstractWriteHandle contentHandle) {
        Document doc;
        try {
            doc = domHelper.extractDocument(contentHandle, null);
        } catch (Exception ex) {
            Util.MAIN_LOGGER.warn("Unable to select text to split; cause: {}", ex.getMessage());
            return null;
        }

        return selectTextInDocument(doc);
    }

    private String selectTextInDocument(Document doc) {
        NodeList items;
        try {
            items = (NodeList) this.textExpression.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format(
                "Unable to evaluate XPath expression for selecting text to split: %s; cause: %s", textExpression, e.getMessage()), e);
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < items.getLength(); i++) {
            if (i > 0) {
                result.append(JOIN_DELIMITER);
            }
            // For now, always getting the text content. We can eventually support an option for serializing the node
            // to a string instead, for use cases where the node may include mixed content.
            String text = items.item(i).getTextContent();
            if (text != null) {
                result.append(text);
            }
        }
        return result.toString();
    }
}
