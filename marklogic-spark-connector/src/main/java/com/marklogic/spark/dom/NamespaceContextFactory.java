/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.dom;

import com.marklogic.spark.Options;
import com.marklogic.spark.Util;

import javax.xml.namespace.NamespaceContext;
import java.util.HashMap;
import java.util.Map;

public interface NamespaceContextFactory {

    static NamespaceContext makeDefaultNamespaceContext() {
        Map<String, String> prefixesToNamespaces = new HashMap<>();
        prefixesToNamespaces.put("model", Util.DEFAULT_XML_NAMESPACE);
        return new XPathNamespaceContext(prefixesToNamespaces);
    }

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
