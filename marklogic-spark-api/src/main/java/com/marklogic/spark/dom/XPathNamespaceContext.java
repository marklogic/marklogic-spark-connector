/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.dom;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import java.util.Iterator;
import java.util.Map;

public class XPathNamespaceContext implements NamespaceContext {

    private final Map<String, String> prefixesToNamespaces;

    public XPathNamespaceContext(Map<String, String> prefixesToNamespaces) {
        this.prefixesToNamespaces = prefixesToNamespaces;
    }

    @Override
    public String getNamespaceURI(String prefix) {
        return prefixesToNamespaces.containsKey(prefix) ?
            prefixesToNamespaces.get(prefix) :
            XMLConstants.NULL_NS_URI;
    }

    @Override
    public String getPrefix(String namespaceURI) {
        // Does not have to be implemented for resolving XPath expressions.
        return null;
    }

    @Override
    public Iterator<String> getPrefixes(String namespaceURI) {
        // Does not have to be implemented for resolving XPath expressions.
        return null;
    }
}
