/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.writer.XmlUtil;
import org.jdom2.*;
import org.jdom2.filter.Filters;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Uses JDOM to select text in an XML document based on an XPath expression.
 */
public class JDOMTextSelector implements TextSelector {

    private final XPathExpression<Object> xpathExpression;
    private final XMLOutputter xmlOutputter;

    // Will make this configurable later.
    private static final String JOIN_DELIMITER = " ";

    public JDOMTextSelector(String xpath) {
        this(xpath, null);
    }

    public JDOMTextSelector(String xpath, Collection<Namespace> namespaces) {
        this.xpathExpression = namespaces != null ?
            XPathFactory.instance().compile(xpath, Filters.fpassthrough(), null, namespaces) :
            XPathFactory.instance().compile(xpath);
        this.xmlOutputter = new XMLOutputter(Format.getPrettyFormat());
    }

    @Override
    public String selectTextToSplit(DocumentWriteOperation operation) {
        Document doc = XmlUtil.extractDocument(operation.getContent());

        List<Object> results;
        try {
            results = this.xpathExpression.evaluate(doc);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to split document using XPath expression: %s; cause: %s",
                xpathExpression.getExpression(), e.getMessage()), e);
        }

        return results.stream().map(this::objectToString).collect(Collectors.joining(JOIN_DELIMITER));
    }

    private String objectToString(Object object) {
        if (object instanceof Text) {
            return ((Text) object).getText();
        }
        if (object instanceof Element) {
            return this.xmlOutputter.outputString((Element) object);
        }
        if (object instanceof Attribute) {
            return ((Attribute) object).getValue();
        }
        if (object instanceof Document) {
            return this.xmlOutputter.outputString((Document) object);
        }
        return object.toString();
    }
}
