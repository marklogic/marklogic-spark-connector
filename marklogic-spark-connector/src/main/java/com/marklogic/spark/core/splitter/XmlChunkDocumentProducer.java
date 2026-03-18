/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.ChunkInputs;
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

class XmlChunkDocumentProducer extends AbstractChunkDocumentProducer {

    private static final String DEFAULT_CHUNKS_ELEMENT_NAME = "chunks";

    private final DOMHelper domHelper;
    private final XmlChunkConfig xmlChunkConfig;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();
    private final DocumentBuilderFactory documentBuilderFactory;

    XmlChunkDocumentProducer(DocumentWriteOperation sourceDocument, Format sourceDocumentFormat,
                             List<ChunkInputs> chunkInputsList, ChunkConfig chunkConfig) {
        super(sourceDocument, sourceDocumentFormat, chunkInputsList, chunkConfig);

        // Namespaces aren't needed for producing chunks.
        this.domHelper = new DOMHelper(null);

        // In 3.0.0, fixed bug where embeddingName was not being honored.
        this.xmlChunkConfig = new XmlChunkConfig(null, chunkConfig.getEmbeddingName(),
            chunkConfig.getEmbeddingXmlNamespace(), null, chunkConfig.isBase64EncodeVectors());

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

        List<Chunk> addedChunks = new ArrayList<>();
        for (int i = 0; i < this.maxChunksPerDocument && hasNext(); i++) {
            ChunkInputs chunkInputs = chunkInputsList.get(listIndex);
            DOMChunk chunk = addChunk(doc, chunkInputs, chunksElement);
            addedChunks.add(chunk);
            listIndex++;
        }

        final String chunkDocumentUri = makeChunkDocumentUri(sourceDocument, "xml");
        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(chunkDocumentUri, chunkConfig.getMetadata(), new DOMHandle(doc)),
            addedChunks
        );
    }

    protected DocumentWriteOperation addChunksToSourceDocument() {
        Document doc = domHelper.extractDocument(super.sourceDocument);

        Element chunksElement = doc.createElementNS(chunkConfig.getXmlNamespace(), determineChunksElementName(doc));
        doc.getDocumentElement().appendChild(chunksElement);

        List<Chunk> addedChunks = new ArrayList<>();
        for (ChunkInputs chunkInputs : chunkInputsList) {
            DOMChunk chunk = addChunk(doc, chunkInputs, chunksElement);
            addedChunks.add(chunk);
        }

        return new DocumentAndChunks(
            new DocumentWriteOperationImpl(sourceDocument.getUri(), sourceDocument.getMetadata(), new DOMHandle(doc)),
            addedChunks
        );
    }

    private Element getClassificationResponseElement(byte[] classificationBytes) {
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document classificationResponse = builder.parse(new ByteArrayInputStream(classificationBytes));
            return classificationResponse.getDocumentElement();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceDocument.getUri(), e.getMessage()), e);
        }
    }

    private DOMChunk addChunk(Document doc, ChunkInputs chunkInputs, Element chunksElement) {
        Element chunk = doc.createElementNS(chunkConfig.getXmlNamespace(), "chunk");
        chunksElement.appendChild(chunk);

        Element text = doc.createElementNS(chunkConfig.getXmlNamespace(), "text");
        text.setTextContent(chunkInputs.getText());
        chunk.appendChild(text);

        if (chunkInputs.getClassification() != null) {
            Element classificationResponse = getClassificationResponseElement(chunkInputs.getClassification());
            Node classificationNode = doc.createElement("classification");
            chunk.appendChild(classificationNode);
            for (int i = 0; i < classificationResponse.getChildNodes().getLength(); i++) {
                Node childNode = classificationResponse.getChildNodes().item(i);
                classificationNode.appendChild(doc.importNode(childNode, true));
            }
        }

        if (chunkInputs.getMetadata() != null) {
            Element metadataElement = doc.createElementNS(chunkConfig.getXmlNamespace(), "chunk-metadata");
            // Re: possibly converting JSON to XML - Copilot recommends using the serialized string, as there's no
            // "correct" way for converting JSON to XML, particularly in regard to arrays. If the user wants XML
            // documents, they can always e.g. use a REST transform to determine how they want to represent the JSON
            // as XML.
            metadataElement.setTextContent(chunkInputs.getMetadata().toString());
            chunk.appendChild(metadataElement);
        }

        var domChunk = new DOMChunk(doc, chunk, this.xmlChunkConfig, this.xPathFactory);
        if (chunkInputs.getEmbedding() != null) {
            domChunk.addEmbedding(chunkInputs.getEmbedding(), chunkInputs.getModelName());
        }
        return domChunk;
    }

    private String determineChunksElementName(Document doc) {
        return doc.getDocumentElement().getElementsByTagNameNS(Util.DEFAULT_XML_NAMESPACE, DEFAULT_CHUNKS_ELEMENT_NAME).getLength() == 0 ?
            DEFAULT_CHUNKS_ELEMENT_NAME : "splitter-chunks";
    }
}
