/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.dom.DOMHelper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Knows how to build a multi-article request and parse a structured document response. The actual call to Semaphore
 * is hidden behind the {@code MultiArticleClassifier} interface so that it can be mocked for testing purposes.
 */
class SemaphoreTextClassifier implements TextClassifier {

    static final Logger SEMAPHORE_LOGGER = LoggerFactory.getLogger("com.marklogic.semaphore.classifier");

    private final MultiArticleClassifier multiArticleClassifier;
    private final DOMHelper domHelper;
    private final Transformer transformer;
    private final String encoding;
    private final XPathExpression articleExpression;

    SemaphoreTextClassifier(MultiArticleClassifier multiArticleClassifier, String encoding) {
        this.multiArticleClassifier = multiArticleClassifier;
        this.domHelper = new DOMHelper(null);
        this.transformer = newTransformer();
        this.encoding = encoding;
        this.articleExpression = domHelper.compileXPath("//ARTICLE", "Unable to evaluate XPath expression");
    }

    @Override
    public void classifyText(List<ClassifiableContent> classifiableContents) {
        Document doc = buildMultiArticleRequest(classifiableContents);
        byte[] documentBytes = convertNodeIntoBytes(doc);

        Document structuredDocument;
        try {
            structuredDocument = multiArticleClassifier.classifyArticles(documentBytes);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify content, cause: %s", e.getMessage()), e);
        }

        if (SEMAPHORE_LOGGER.isTraceEnabled()) {
            SEMAPHORE_LOGGER.trace("Structured document response: {}", DOMHelper.prettyPrintNode(structuredDocument));
        }

        addArticlesToContents(classifiableContents, structuredDocument);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(multiArticleClassifier);
    }

    private Document buildMultiArticleRequest(List<ClassifiableContent> classifiableContents) {
        Document doc = domHelper.newDocument();
        Element root = doc.createElement("STRUCTUREDDOCUMENT");
        doc.appendChild(root);
        for (ClassifiableContent content : classifiableContents) {
            Element article = doc.createElement("ARTICLE");
            article.setTextContent(content.getTextToClassify().trim());
            root.appendChild(article);
        }
        if (SEMAPHORE_LOGGER.isTraceEnabled()) {
            SEMAPHORE_LOGGER.trace("Multi-article request: {}", DOMHelper.prettyPrintNode(doc));
        }
        return doc;
    }

    private byte[] convertNodeIntoBytes(Node doc) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Result result = new StreamResult(baos);
        try {
            this.transformer.transform(new DOMSource(doc), result);
            return baos.toByteArray();
        } catch (TransformerException e) {
            throw new ConnectorException(String.format("Unable to generate XML; cause: %s", e.getMessage()), e);
        }
    }

    private void addArticlesToContents(List<ClassifiableContent> classifiableContents, Document structuredDocument) {
        NodeList articles;
        try {
            articles = (NodeList) articleExpression.evaluate(structuredDocument, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new ConnectorException(String.format(
                "Unable to retrieve articles from classification response; cause: %s", e.getMessage()), e);
        }
        for (int i = 0; i < articles.getLength(); i++) {
            byte[] articleBytes = convertNodeIntoBytes(articles.item(i));
            if (i >= classifiableContents.size()) {
                Util.MAIN_LOGGER.warn("Received {} articles but only have {} chunks.", articles.getLength(), classifiableContents.size());
            } else {
                classifiableContents.get(i).addClassification(articleBytes);
            }
        }
    }

    private Transformer newTransformer() {
        try {
            final Transformer t = DOMHelper.newTransformerFactory().newTransformer();
            if (this.encoding != null) {
                t.setOutputProperty(OutputKeys.ENCODING, this.encoding);
            } else {
                t.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            }

            // Semaphore needs the XML declaration present.
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");

            // No need to waste time indenting XML, Semaphore doesn't need it.
            t.setOutputProperty(OutputKeys.INDENT, "no");
            return t;
        } catch (TransformerConfigurationException e) {
            throw new ConnectorException(
                String.format("Unable to instantiate transformer for classifying text; cause: %s", e.getMessage()), e
            );
        }
    }
}
