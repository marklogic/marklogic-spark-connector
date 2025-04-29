/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.dom;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.Objects;

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
        Objects.requireNonNull(xml);
        return parseXmlString(xml, sourceUri);
    }

    public Document parseXmlString(String xml, String sourceUri) {
        try {
            return getDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        } catch (Exception e) {
            String message = sourceUri != null ?
                String.format("Unable to parse XML for document with URI: %s; cause: %s", sourceUri, e.getMessage()) :
                String.format("Unable to parse XML; cause: %s", e.getMessage());
            throw new ConnectorException(message, e);
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
            throw new ConnectorException(String.format(
                "Unable to compile XPath expression for %s: %s; cause: %s",
                purposeForErrorMessage, xpathExpression, message), e
            );
        }
    }

    public static TransformerFactory newTransformerFactory() throws TransformerConfigurationException {
        TransformerFactory factory = TransformerFactory.newInstance();
        // Disables certain features as recommended by Sonar to prevent security vulnerabilities.
        // Also see https://stackoverflow.com/questions/32178558/how-to-prevent-xml-external-entity-injection-on-transformerfactory .
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        return factory;
    }

    // Only intended for trace logging and manual debugging/testing.
    public static String prettyPrintNode(Node node) {
        try {
            final Transformer t = newTransformerFactory().newTransformer();
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.setOutputProperty(OutputKeys.INDENT, "yes");

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Result result = new StreamResult(baos);
            t.transform(new DOMSource(node), result);
            return new String(baos.toByteArray());
        } catch (Exception ex) {
            throw new ConnectorException(ex.getMessage(), ex);
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
                throw new ConnectorException(String.format("Unable to create XML document; cause: %s", e.getMessage()), e);
            }
        }
        return this.documentBuilder;
    }
}
