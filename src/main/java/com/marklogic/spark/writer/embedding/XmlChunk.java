/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.Text;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.util.Collection;

public class XmlChunk implements Chunk {

    private final Element chunk;
    private final XPathExpression<Text> textXPathExpression;
    private final String embeddingName;
    private final String embeddingNamespace;

    public XmlChunk(Element chunk, String textXPathExpression, String embeddingName, String embeddingNamespace, Collection<Namespace> namespaces) {
        this.chunk = chunk;

        String xpath = textXPathExpression != null ? textXPathExpression : "node()[local-name(.) = 'text']/text()";
        this.textXPathExpression = namespaces != null ?
            XPathFactory.instance().compile(xpath, Filters.text(), null, namespaces) :
            XPathFactory.instance().compile(xpath, Filters.text());

        this.embeddingName = embeddingName != null ? embeddingName : "embedding";
        this.embeddingNamespace = embeddingNamespace;
    }

    @Override
    public String getEmbeddingText() {
        Text text = textXPathExpression.evaluateFirst(this.chunk);
        // Need a test for not finding the text.
        return text.getText();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        Element el = embeddingNamespace != null ? new Element(embeddingName, embeddingNamespace) : new Element(embeddingName);
        chunk.addContent(el.setText(embedding.vectorAsList().toString()));
    }
}
