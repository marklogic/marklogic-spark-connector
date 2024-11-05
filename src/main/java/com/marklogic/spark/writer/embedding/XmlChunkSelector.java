/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.spark.writer.XmlUtil;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class XmlChunkSelector implements ChunkSelector {

    private final XPathExpression<Element> chunksXPathExpression;
    private final String textXPathExpression;
    private final String embeddingName;
    private final String embeddingNamespace;
    private final Collection<Namespace> xpathNamespaces;

    static class Builder {
        private String chunksXPathExpression;
        private String textXPathExpression;
        private String embeddingName;
        private String embeddingNamespace;
        private Collection<Namespace> xpathNamespaces;

        public XmlChunkSelector build() {
            String tmp = chunksXPathExpression != null ? chunksXPathExpression : "/node()/chunks";
            XPathExpression<Element> chunksExpression = XPathFactory.instance().compile(tmp, Filters.element(), null, xpathNamespaces);
            return new XmlChunkSelector(chunksExpression, textXPathExpression, embeddingName, embeddingNamespace, xpathNamespaces);
        }

        public Builder withChunksXPathExpression(String chunksXPathExpression) {
            if (chunksXPathExpression != null) {
                this.chunksXPathExpression = chunksXPathExpression;
            }
            return this;
        }

        public Builder withTextXPathExpression(String textXPathExpression) {
            this.textXPathExpression = textXPathExpression;
            return this;
        }

        public Builder withEmbeddingName(String embeddingName) {
            this.embeddingName = embeddingName;
            return this;
        }

        public Builder withEmbeddingNamespace(String embeddingNamespace) {
            this.embeddingNamespace = embeddingNamespace;
            return this;
        }

        public Builder withXPathNamespaces(Collection<Namespace> xpathNamespaces) {
            this.xpathNamespaces = xpathNamespaces;
            return this;
        }
    }

    private XmlChunkSelector(XPathExpression<Element> chunksXPathExpression, String textXPathExpression,
                             String embeddingName, String embeddingNamespace, Collection<Namespace> xpathNamespaces) {
        this.chunksXPathExpression = chunksXPathExpression;
        this.textXPathExpression = textXPathExpression;
        this.embeddingName = embeddingName;
        this.embeddingNamespace = embeddingNamespace;
        this.xpathNamespaces = xpathNamespaces;
    }

    @Override
    public DocumentAndChunks selectChunks(DocumentWriteOperation sourceDocument) {
        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());

        List<Element> elements = chunksXPathExpression.evaluate(doc);
        if (elements == null || elements.isEmpty()) {
            return new ChunkSelector.DocumentAndChunks(sourceDocument, null);
        }

        List<Chunk> chunks = elements.stream()
            .map(element -> new XmlChunk(sourceDocument.getUri(), element, textXPathExpression, embeddingName, embeddingNamespace, xpathNamespaces))
            .collect(Collectors.toList());

        return new ChunkSelector.DocumentAndChunks(
            new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JDOMHandle(doc)),
            chunks
        );
    }
}
