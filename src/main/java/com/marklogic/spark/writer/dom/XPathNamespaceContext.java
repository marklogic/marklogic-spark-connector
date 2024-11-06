/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.dom;

import com.marklogic.spark.Options;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class XPathNamespaceContext implements NamespaceContext {

    private final Map<String, String> prefixesToNamespaces;

    public XPathNamespaceContext(Map<String, String> properties) {
        prefixesToNamespaces = new HashMap<>();
        properties.keySet().stream()
            .filter(key -> key.startsWith(Options.XPATH_NAMESPACE_PREFIX))
            .forEach(key -> {
                String prefix = key.substring(Options.XPATH_NAMESPACE_PREFIX.length());
                String namespace = properties.get(key);
                prefixesToNamespaces.put(prefix, namespace);
            });
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
