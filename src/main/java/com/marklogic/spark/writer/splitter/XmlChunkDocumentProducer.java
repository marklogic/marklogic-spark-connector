/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.writer.dom.DOMHelper;
import com.marklogic.spark.writer.embedding.Chunk;
import com.marklogic.spark.writer.embedding.DOMChunk;
import com.marklogic.spark.writer.embedding.EmbeddingGenerator;
import com.marklogic.spark.writer.embedding.XmlChunkConfig;
import dev.langchain4j.data.segment.TextSegment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.List;

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ELEMENT_NAME = "chunks";

    private final EmbeddingGenerator embeddingGenerator;
    private final DOMHelper domHelper;
    private final XmlChunkConfig xmlChunkConfig;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<TextSegment> textSegments, ChunkConfig chunkConfig, EmbeddingGenerator embeddingGenerator) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);
        this.embeddingGenerator = embeddingGenerator;

        // Namespaces aren't needed for producing chunks.
        this.domHelper = new DOMHelper(null);
        this.xmlChunkConfig = new XmlChunkConfig();
    }

    @Override
    protected DocumentWriteOperation makeChunkDocument() {
        Document doc = this.domHelper.newDocument();
        Element root = doc.createElementNS(
            chunkConfig.getXmlNamespace(),
            chunkConfig.getRootName() != null ? chunkConfig.getRootName() : "root"
        );

        doc.appendChild(root);
        Element sourceUri = doc.createElementNS(chunkConfig.getXmlNamespace(), "source-uri");
        sourceUri.setTextContent(sourceDocument.getUri());
        root.appendChild(sourceUri);

        Element chunksElement = doc.createElementNS(chunkConfig.getXmlNamespace(), DEFAULT_CHUNKS_ELEMENT_NAME);
        root.appendChild(chunksElement);

        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            addChunk(doc, textSegments.get(listIndex++), chunksElement, chunks);
        }
        addEmbeddingsToChunks(chunks);

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "xml");
        return new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new DOMHandle(doc));
    }

    protected DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = domHelper.extractDocument(super.sourceDocument);

        Element chunksElement = doc.createElementNS(chunkConfig.getXmlNamespace(), determineChunksElementName(doc));
        doc.getDocumentElement().appendChild(chunksElement);

        List<Chunk> chunks = new ArrayList<>();
        for (TextSegment textSegment : textSegments) {
            addChunk(doc, textSegment, chunksElement, chunks);
        }
        addEmbeddingsToChunks(chunks);

        return new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new DOMHandle(doc));
    }

    private void addChunk(Document doc, TextSegment textSegment, Element chunksElement, List<Chunk> chunks) {
        Element chunk = doc.createElementNS(chunkConfig.getXmlNamespace(), "chunk");
        chunksElement.appendChild(chunk);
        Element text = doc.createElementNS(chunkConfig.getXmlNamespace(), "text");
        text.setTextContent(textSegment.text());
        chunk.appendChild(text);
        chunks.add(new DOMChunk(super.sourceDocument.getUri(), doc, chunk, this.xmlChunkConfig, this.xPathFactory));
    }

    private void addEmbeddingsToChunks(List<Chunk> chunks) {
        if (this.embeddingGenerator != null) {
            this.embeddingGenerator.addEmbeddings(chunks);
        }
    }

    private String determineChunksElementName(Document doc) {
        return doc.getDocumentElement().getElementsByTagName(DEFAULT_CHUNKS_ELEMENT_NAME).getLength() == 0 ?
            DEFAULT_CHUNKS_ELEMENT_NAME : "splitter-chunks";
    }
}
