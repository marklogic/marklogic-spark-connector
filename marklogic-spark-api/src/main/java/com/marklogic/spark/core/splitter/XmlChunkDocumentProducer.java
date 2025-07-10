/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DOMChunk;
import com.marklogic.spark.core.embedding.DocumentAndChunks;
import com.marklogic.spark.core.embedding.XmlChunkConfig;
import com.marklogic.spark.dom.DOMHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ELEMENT_NAME = "chunks";

    private final DOMHelper domHelper;
    private final XmlChunkConfig xmlChunkConfig;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();
    private final DocumentBuilderFactory documentBuilderFactory;

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<String> textSegments, ChunkConfig chunkConfig, List<byte[]> classifications, List<float[]> embeddings) {
        super(sourceDocument, sourceDocumentFormat, textSegments, chunkConfig, classifications, embeddings);

        // Namespaces aren't needed for producing chunks.
        this.domHelper = new DOMHelper(null);
        this.xmlChunkConfig = new XmlChunkConfig(null, null, chunkConfig.getEmbeddingXmlNamespace(), null, chunkConfig.isBase64EncodeVectors());
        documentBuilderFactory = DocumentBuilderFactory.newInstance();
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
        int chunksCounter = 0;
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            Element classificationReponseNode = getNthClassificationResponseElement(chunksCounter++);
            float[] embedding = getEmbeddingIfExists(embeddings, listIndex);
            addChunk(doc, textSegments.get(listIndex++), chunksElement, chunks, classificationReponseNode, embedding);
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
        AtomicInteger ct = new AtomicInteger(0);
        for (String textSegment : textSegments) {
            int segCounter = ct.getAndIncrement();
            Element classificationReponseNode = getNthClassificationResponseElement(segCounter);
            float[] embedding = getEmbeddingIfExists(embeddings, segCounter);
            addChunk(doc, textSegment, chunksElement, chunks, classificationReponseNode, embedding);
        }

        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new DOMHandle(doc)),
            chunks
        );
    }

    private Element getNthClassificationResponseElement(int n) {
        if (classifications != null && !classifications.isEmpty()) {
            byte[] classificationBytes = classifications.get(n);
            try {
                DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
                Document classificationResponse = builder.parse(new ByteArrayInputStream(classificationBytes));
                return classificationResponse.getDocumentElement();
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceDocument.getUri(), e.getMessage()), e);
            }
        } else {
            return null;
        }
    }

    private void addChunk(Document doc, String textSegment, Element chunksElement, List<Chunk> chunks, Element classificationResponse, float[] embedding) {
        Element chunk = doc.createElementNS(chunkConfig.getXmlNamespace(), "chunk");
        chunksElement.appendChild(chunk);

        Element text = doc.createElementNS(chunkConfig.getXmlNamespace(), "text");
        text.setTextContent(textSegment);
        chunk.appendChild(text);

        if (classificationResponse != null) {
            Node classificationNode = doc.createElement("classification");
            chunk.appendChild(classificationNode);
            for (int i = 0; i < classificationResponse.getChildNodes().getLength(); i++) {
                Node childNode = classificationResponse.getChildNodes().item(i);
                classificationNode.appendChild(doc.importNode(childNode, true));
            }
        }

        var domChunk = new DOMChunk(super.sourceDocument.getUri(), doc, chunk, this.xmlChunkConfig, this.xPathFactory);
        if (embedding != null) {
            domChunk.addEmbedding(embedding);
        }
        chunks.add(domChunk);
    }

    private String determineChunksElementName(Document doc) {
        return doc.getDocumentElement().getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, DEFAULT_CHUNKS_ELEMENT_NAME).getLength() == 0 ?
            DEFAULT_CHUNKS_ELEMENT_NAME : "splitter-chunks";
    }
}
