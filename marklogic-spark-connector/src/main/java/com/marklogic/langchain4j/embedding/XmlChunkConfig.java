/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import javax.xml.namespace.NamespaceContext;

public class XmlChunkConfig {

    // The default expression ignores the namespace so that if a user is e.g. constructing a new XML document with a
    // custom namespace but still uses 'text' as the name of the text element in a chunk, the text can still be found.
    private static final String DEFAULT_TEXT_EXPRESSION = "node()[local-name(.) = 'text']";

    private final String textExpression;
    private final String embeddingName;
    private final String embeddingNamespace;
    private final NamespaceContext namespaceContext;

    // Defaults to the config used by the connector's splitter feature.
    public XmlChunkConfig() {
        this(null, null, null, null);
    }

    public XmlChunkConfig(String textExpression, String embeddingName, String embeddingNamespace, NamespaceContext namespaceContext) {
        this.textExpression = textExpression != null ? textExpression : DEFAULT_TEXT_EXPRESSION;
        this.embeddingName = embeddingName != null ? embeddingName : "embedding";
        this.embeddingNamespace = embeddingNamespace;
        this.namespaceContext = namespaceContext;
    }

    public String getTextExpression() {
        return textExpression;
    }

    String getEmbeddingName() {
        return embeddingName;
    }

    String getEmbeddingNamespace() {
        return embeddingNamespace;
    }

    NamespaceContext getNamespaceContext() {
        return namespaceContext;
    }
}
