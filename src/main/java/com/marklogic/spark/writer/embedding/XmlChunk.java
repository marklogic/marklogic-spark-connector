/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import dev.langchain4j.data.embedding.Embedding;
import org.jdom2.Element;
import org.jdom2.Text;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

public class XmlChunk implements Chunk {

    private final Element chunk;
    private final XPathExpression<Text> textXPathExpression;
    // Include namespace?
    private final String embeddingArrayName;

    public XmlChunk(Element chunk) {
        this(chunk, XPathFactory.instance().compile("text/text()", Filters.text()), "embedding");
    }

    public XmlChunk(Element chunk, XPathExpression<Text> textXPathExpression, String embeddingArrayName) {
        this.chunk = chunk;
        this.textXPathExpression = textXPathExpression;
        this.embeddingArrayName = embeddingArrayName;
    }

    @Override
    public String getEmbeddingText() {
        Text text = textXPathExpression.evaluateFirst(this.chunk);
        // Need a test for not finding the text.
        return text.getText();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        chunk.addContent(new Element(embeddingArrayName).setText(embedding.vectorAsList().toString()));
    }
}
