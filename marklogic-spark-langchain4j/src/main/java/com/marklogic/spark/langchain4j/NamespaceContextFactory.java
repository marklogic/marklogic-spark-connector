/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.langchain4j.dom.XPathNamespaceContext;
import com.marklogic.spark.Options;

import javax.xml.namespace.NamespaceContext;
import java.util.HashMap;
import java.util.Map;

public interface NamespaceContextFactory {

    static NamespaceContext makeNamespaceContext(Map<String, String> properties) {
        Map<String, String> prefixesToNamespaces = new HashMap<>();
        properties.keySet().stream()
            .filter(key -> key.startsWith(Options.XPATH_NAMESPACE_PREFIX))
            .forEach(key -> {
                String prefix = key.substring(Options.XPATH_NAMESPACE_PREFIX.length());
                String namespace = properties.get(key);
                prefixesToNamespaces.put(prefix, namespace);
            });
        return new XPathNamespaceContext(prefixesToNamespaces);
    }
}
