/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.langchain4j.Util;
import com.marklogic.langchain4j.classifier.TextClassifier;
import com.marklogic.langchain4j.dom.DOMHelper;
import com.marklogic.langchain4j.embedding.Chunk;
import com.marklogic.langchain4j.embedding.DOMChunk;
import com.marklogic.langchain4j.embedding.DocumentAndChunks;
import com.marklogic.langchain4j.embedding.XmlChunkConfig;
import com.smartlogic.classificationserver.client.ClassificationScore;
import dev.langchain4j.data.segment.TextSegment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPathFactory;
import java.util.*;

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ELEMENT_NAME = "chunks";

    private final DOMHelper domHelper;
    private final XmlChunkConfig xmlChunkConfig;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<TextSegment> textSegments, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig);

        // Namespaces aren't needed for producing chunks.
        this.domHelper = new DOMHelper(null);
        this.xmlChunkConfig = new XmlChunkConfig(chunkConfig.getEmbeddingXmlNamespace());
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

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "xml");
        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new DOMHandle(doc)),
            chunks
        );
    }

    protected DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = domHelper.extractDocument(super.sourceDocument);

        Element chunksElement = doc.createElementNS(chunkConfig.getXmlNamespace(), determineChunksElementName(doc));
        doc.getDocumentElement().appendChild(chunksElement);

        List<Chunk> chunks = new ArrayList<>();
        for (TextSegment textSegment : textSegments) {
            addChunk(doc, textSegment, chunksElement, chunks);
        }

        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new DOMHandle(doc)),
            chunks
        );
    }

    private void addChunk(Document doc, TextSegment textSegment, Element chunksElement, List<Chunk> chunks) {
        Element chunk = doc.createElementNS(chunkConfig.getXmlNamespace(), "chunk");
        chunksElement.appendChild(chunk);
        Element text = doc.createElementNS(chunkConfig.getXmlNamespace(), "text");
        text.setTextContent(textSegment.text());
        chunk.appendChild(text);
        if (chunkConfig.getClassifier() != null) {
            chunk.appendChild(generateConceptsForChunk(doc, textSegment));
        }
        chunks.add(new DOMChunk(super.sourceDocument.getUri(), doc, chunk, this.xmlChunkConfig, this.xPathFactory));
    }

    private String determineChunksElementName(Document doc) {
        return doc.getDocumentElement().getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, DEFAULT_CHUNKS_ELEMENT_NAME).getLength() == 0 ?
            DEFAULT_CHUNKS_ELEMENT_NAME : "splitter-chunks";
    }

    private Element generateConceptsForChunk(Document doc, TextSegment textSegment) {
        TextClassifier classifier = chunkConfig.getClassifier();
        Map<String, Collection<ClassificationScore>> classificationScores = classifier.classifyText(super.sourceDocument.getUri(), textSegment.text());
        Element conceptsElement = doc.createElementNS(chunkConfig.getXmlNamespace(), classifier.getConceptsArrayName());
        for (Map.Entry<String, Collection<ClassificationScore>> entry : classificationScores.entrySet()) {
            for (ClassificationScore classificationScore : classificationScores.get(entry.getKey())) {
                Element conceptElement = doc.createElementNS(chunkConfig.getXmlNamespace(), "concept");
                conceptElement.setAttribute("id", classificationScore.getId());
                conceptElement.setAttribute("name", entry.getKey());
                conceptElement.setAttribute("value", classificationScore.getName());
                conceptElement.setAttribute("score", Float.toString(classificationScore.getScore()));
                conceptsElement.appendChild(conceptElement);
            }
        }
        return conceptsElement;
    }
}
