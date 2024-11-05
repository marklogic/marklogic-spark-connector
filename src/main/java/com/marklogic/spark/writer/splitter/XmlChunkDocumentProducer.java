/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.extra.jdom.JDOMHandle;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.spark.writer.XmlUtil;
import com.marklogic.spark.writer.embedding.Chunk;
import com.marklogic.spark.writer.embedding.EmbeddingGenerator;
import com.marklogic.spark.writer.embedding.XmlChunk;
import dev.langchain4j.data.segment.TextSegment;
import org.jdom2.Document;
import org.jdom2.Element;

import java.util.ArrayList;
import java.util.List;

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ELEMENT_NAME = "chunks";

    private final EmbeddingGenerator embeddingGenerator;

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<TextSegment> textSegments, ChunkConfig chunkConfig, EmbeddingGenerator embeddingGenerator) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
        this.embeddingGenerator = embeddingGenerator;
    }

    @Override
    protected DocumentWriteOperation makeChunkDocument() {
        Document doc = new Document();
        Element root = newElement(chunkConfig.getRootName() != null ? chunkConfig.getRootName() : "root");
        doc.addContent(root);
        root.addContent(newElement("source-uri").addContent(sourceDocument.getUri()));

        final Element chunksElement = newElement(DEFAULT_CHUNKS_ELEMENT_NAME);
        doc.getRootElement().addContent(chunksElement);

        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            addChunk(textSegments.get(listIndex++), chunksElement, chunks);
        }
        addEmbeddingsToChunks(chunks);

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "xml");
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new JDOMHandle(doc));
    }

    protected DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = XmlUtil.extractDocument(sourceDocument.getContent());

        final Element chunksElement = new Element(determineChunksElementName(doc));
        doc.getRootElement().addContent(chunksElement);

        List<Chunk> chunks = new ArrayList<>();
        for (TextSegment textSegment : textSegments) {
            addChunk(textSegment, chunksElement, chunks);
        }
        addEmbeddingsToChunks(chunks);

        return new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new JDOMHandle(doc));
    }

    private void addChunk(TextSegment textSegment, Element chunksElement, List<Chunk> chunks) {
        Element chunk = newElement("chunk").addContent(newElement("text").addContent(textSegment.text()));
        chunksElement.addContent(chunk);
        chunks.add(new XmlChunk(super.sourceDocument.getUri(), chunk, null, null, chunkConfig.getXmlNamespace(), null));
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        if (this.embeddingGenerator != null) {
            this.embeddingGenerator.addEmbeddings(chunks);
        }
    }

    private String determineChunksElementName(Document doc) {
        return doc.getRootElement().getChildren(DEFAULT_CHUNKS_ELEMENT_NAME).isEmpty() ?
            DEFAULT_CHUNKS_ELEMENT_NAME : "splitter-chunks";
    }

    private Element newElement(String name) {
        String ns = chunkConfig.getXmlNamespace();
        return ns != null ? new Element(name, ns) : new Element(name);
    }
}
