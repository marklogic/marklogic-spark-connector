/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.dom;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.langchain4j.MarkLogicLangchainException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;

/**
 * Simplifies operations with the Java DOM API.
 */
public class DOMHelper {

    private final DocumentBuilderFactory documentBuilderFactory;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();
    private final NamespaceContext namespaceContext;
    private DocumentBuilder documentBuilder;

    public DOMHelper(NamespaceContext namespaceContext) {
        // This can be reused for multiple calls, which will only be in the context of a single partition writer and
        // thus we don't need to worry about thread safety for it.
        this.documentBuilderFactory = DocumentBuilderFactory.newInstance();
        this.documentBuilderFactory.setNamespaceAware(true);
        this.namespaceContext = namespaceContext;
    }

    public Document extractDocument(DocumentWriteOperation sourceDocument) {
        return extractDocument(sourceDocument.getContent(), sourceDocument.getUri());
    }

    public Document extractDocument(AbstractWriteHandle handle, String sourceUri) {
        if (handle instanceof DOMHandle) {
            return ((DOMHandle) handle).get();
        }

        String xml = HandleAccessor.contentAsString(handle);

        try {
            return getDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        } catch (Exception e) {
            throw new MarkLogicLangchainException(String.format("Unable to parse XML for document with URI: %s; cause: %s",
                sourceUri, e.getMessage()), e);
        }
    }

    public Document newDocument() {
        return getDocumentBuilder().newDocument();
    }

    public XPathExpression compileXPath(String xpathExpression, String purposeForErrorMessage) {
        XPath xpath = this.xPathFactory.newXPath();
        if (namespaceContext != null) {
            xpath.setNamespaceContext(namespaceContext);
        }

        try {
            return xpath.compile(xpathExpression);
        } catch (XPathExpressionException e) {
            String message = massageXPathCompilationError(e.getMessage());
            throw new MarkLogicLangchainException(String.format(
                "Unable to compile XPath expression for %s: %s; cause: %s",
                purposeForErrorMessage, xpathExpression, message), e
            );
        }
    }

    private String massageXPathCompilationError(String message) {
        String unnecessaryPart = "javax.xml.transform.TransformerException: ";
        if (message.startsWith(unnecessaryPart)) {
            return message.substring(unnecessaryPart.length());
        }
        return message;
    }

    private DocumentBuilder getDocumentBuilder() {
        if (this.documentBuilder == null) {
            try {
                this.documentBuilder = this.documentBuilderFactory.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new MarkLogicLangchainException(String.format("Unable to create XML document; cause: %s", e.getMessage()), e);
            }
        }
        return this.documentBuilder;
    }
}
