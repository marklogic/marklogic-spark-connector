/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.extraction.ExtractionUtil;
import com.marklogic.spark.core.splitter.ChunkAssembler;
import com.marklogic.spark.dom.DOMHelper;
import com.marklogic.spark.dom.NamespaceContextFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Knows how to build instances of {@code DocumentWriteOperation} based on the data in {@code DocBuilder.DocumentInputs}.
 * The latter is expected to contain data read from a Spark row, normalized into a standard set of inputs regardless
 * of the schema of the Spark row.
 */
public class DocBuilder {

    public interface UriMaker {
        String makeURI(String initialUri, JsonNode uriTemplateValues);
    }

    public static class ExtractedTextConfig {
        private final Format format;
        private final DocumentMetadataHandle metadata;
        private final boolean dropSource;

        public ExtractedTextConfig(Format format, DocumentMetadataHandle metadata, boolean dropSource) {
            this.format = format;
            this.metadata = metadata;
            this.dropSource = dropSource;
        }
    }

    private final UriMaker uriMaker;
    private final DocumentMetadataHandle metadataFromOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DOMHelper domHelper = new DOMHelper(NamespaceContextFactory.makeDefaultNamespaceContext());
    private final ExtractedTextConfig extractedTextConfig;
    private final ChunkAssembler chunkAssembler;
    private DocumentBuilder documentBuilder;
    private final XmlMapper xmlMapper;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadata, ExtractedTextConfig extractedTextConfig, ChunkAssembler chunkAssembler) {
        this.uriMaker = uriMaker;
        this.metadataFromOptions = metadata;
        this.extractedTextConfig = extractedTextConfig;
        this.chunkAssembler = chunkAssembler;
        xmlMapper = new XmlMapper();
    }

    /**
     * @param inputs set of inputs constructed from a single Spark row.
     * @return one or more documents to write to MarkLogic, based on the inputs object.
     */
    Collection<DocumentWriteOperation> buildDocuments(DocumentInputs inputs) {
        // Using a map to ensure we don't have 2+ documents with the same URI. Some operations below will want to
        // overwrite an entry in a map, which is perfectly fine.
        final Map<String, DocumentWriteOperation> documents = new LinkedHashMap<>();
        DocumentWriteOperation mainDocument = buildMainDocument(inputs);
        documents.put(mainDocument.getUri(), mainDocument);

        DocumentWriteOperation extractedTextDoc = buildExtractedTextDocument(inputs, mainDocument);
        if (extractedTextDoc != null) {
            documents.put(extractedTextDoc.getUri(), extractedTextDoc);
        }

        buildChunkDocuments(inputs, mainDocument, extractedTextDoc).forEach(doc -> documents.put(doc.getUri(), doc));

        if (extractedTextDoc != null && extractedTextConfig.dropSource) {
            documents.remove(mainDocument.getUri());
        }
        return documents.values();
    }

    private DocumentWriteOperation buildMainDocument(DocumentInputs inputs) {
        final String sourceUri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        final String graph = inputs.getGraph();
        final DocumentMetadataHandle metadataFromRow = inputs.getInitialMetadata();

        AbstractWriteHandle content = inputs.getContent();
        if (inputs.getDocumentClassification() != null && inputs.getExtractedText() == null) {
            content = addClassificationToMainDocument(inputs, sourceUri);
        }

        DocumentMetadataHandle sourceMetadata = metadataFromRow;
        if (sourceMetadata != null) {
            // If the row contains metadata, use it, but first override it based on the metadata specified by user options.
            overrideMetadataFromRowWithMetadataFromOptions(sourceMetadata);
            if (graph != null) {
                sourceMetadata.getCollections().add(graph);
            }
            return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, content);
        }
        // If the row doesn't contain metadata, use the metadata specified by user options. We need to be careful
        // not to modify that object though, as it will be reused on subsequent calls.
        sourceMetadata = metadataFromOptions;
        if (graph != null && !sourceMetadata.getCollections().contains(graph)) {
            sourceMetadata = newMetadataWithGraph(graph);
        }
        return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, content);
    }

    private AbstractWriteHandle addClassificationToMainDocument(DocumentInputs inputs, String sourceUri) {
        AbstractWriteHandle content = inputs.getContent();
        final Format sourceDocumentFormat = Util.determineSourceDocumentFormat(content, sourceUri);
        final byte[] classification = inputs.getDocumentClassification();
        if (Format.XML.equals(sourceDocumentFormat)) {
            Document originalDoc = domHelper.extractDocument(content, inputs.getInitialUri());
            addClassificationToXmlDocument(originalDoc, inputs.getInitialUri(), classification);
            return new DOMHandle(originalDoc);
        } else if (Format.JSON.equals(sourceDocumentFormat)) {
            JsonNode doc = Util.getJsonFromHandle(content);
            addClassificationToJsonDocument((ObjectNode) doc, inputs.getInitialUri(), classification);
            return new JacksonHandle(doc);
        }
        Util.MAIN_LOGGER.warn("Cannot add classification to document with URI {}; document is neither JSON nor XML.", sourceUri);
        return content;
    }

    private void addClassificationToJsonDocument(ObjectNode jsonDocument, String uri, byte[] classification) {
        try {
            JsonNode classificationData = xmlMapper.readTree(classification);
            jsonDocument.set("classification", classificationData);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    public void addClassificationToXmlDocument(Document document, String uri, byte[] classification) {
        try {
            if (documentBuilder == null) {
                documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            }
            Document classificationXml = documentBuilder.parse(new ByteArrayInputStream(classification));
            NodeList articleChildNodes = classificationXml.getElementsByTagName("ARTICLE").item(0).getChildNodes();
            Node classificationNode = document.createElementNS(Util.DEFAULT_XML_NAMESPACE, "classification");
            for (int i = 0; i < articleChildNodes.getLength(); i++) {
                Node importedChildNode = document.importNode(articleChildNodes.item(i), true);
                classificationNode.appendChild(importedChildNode);
            }
            document.getFirstChild().appendChild(classificationNode);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    private DocumentWriteOperation buildExtractedTextDocument(DocumentInputs inputs, DocumentWriteOperation mainDocument) {
        if (inputs.getExtractedText() != null) {
            String sourceUri = mainDocument.getUri();

            DocumentMetadataHandle metadataToUse = this.extractedTextConfig.metadata != null ?
                this.extractedTextConfig.metadata :
                (DocumentMetadataHandle) mainDocument.getMetadata();

            return Format.XML.equals(this.extractedTextConfig.format) ?
                buildExtractedXmlDocument(sourceUri, inputs, metadataToUse) :
                buildExtractedJsonDocument(sourceUri, inputs, metadataToUse);
        }
        return null;
    }

    /**
     * If an instance of {@code DocumentInputs} has metadata specified (i.e. metadata from the Spark row), override it
     * with any metadata specified by the user via options.
     *
     * @param metadataFromRow
     */
    private void overrideMetadataFromRowWithMetadataFromOptions(DocumentMetadataHandle metadataFromRow) {
        if (!metadataFromOptions.getCollections().isEmpty()) {
            metadataFromRow.setCollections(metadataFromOptions.getCollections());
        }
        if (!metadataFromOptions.getPermissions().isEmpty()) {
            metadataFromRow.setPermissions(metadataFromOptions.getPermissions());
        }
        if (metadataFromOptions.getQuality() != 0) {
            metadataFromRow.setQuality(metadataFromOptions.getQuality());
        }
        if (!metadataFromOptions.getProperties().isEmpty()) {
            metadataFromRow.setProperties(metadataFromOptions.getProperties());
        }
        if (!metadataFromOptions.getMetadataValues().isEmpty()) {
            metadataFromRow.setMetadataValues(metadataFromOptions.getMetadataValues());
        }
    }

    /**
     * If a semantics graph is specified in the set of document inputs, must copy the DocumentMetadataHandle instance
     * in this class to a new DocumentMetadataHandle instance that includes the graph as a collection. This is done to
     * avoid modifying the DocumentMetadataHandle instance owned by this class which is expected to be reused for
     * many documents.
     *
     * @param graph
     * @return
     */
    private DocumentMetadataHandle newMetadataWithGraph(String graph) {
        DocumentMetadataHandle newMetadata = new DocumentMetadataHandle();
        newMetadata.getCollections().addAll(metadataFromOptions.getCollections());
        newMetadata.getPermissions().putAll(metadataFromOptions.getPermissions());
        newMetadata.setQuality(metadataFromOptions.getQuality());
        newMetadata.setProperties(metadataFromOptions.getProperties());
        newMetadata.setMetadataValues(metadataFromOptions.getMetadataValues());
        newMetadata.getCollections().add(graph);
        return newMetadata;
    }

    private DocumentWriteOperation buildExtractedJsonDocument(String sourceUri, DocumentInputs inputs, DocumentMetadataHandle sourceMetadata) {
        ObjectNode doc = objectMapper.createObjectNode();
        if (!extractedTextConfig.dropSource) {
            doc.put("source-uri", sourceUri);
        }
        doc.put("content", inputs.getExtractedText());
        if (inputs.getExtractedMetadata() != null) {
            ObjectNode node = doc.putObject("extracted-metadata");
            inputs.getExtractedMetadata().entrySet().forEach(entry -> {
                // Replacing the colon, which is not allowable in an index declaration.
                String key = entry.getKey().replace(":", "-");
                node.put(key, entry.getValue());
            });
        }
        if (inputs.getDocumentClassification() != null) {
            addClassificationToJsonDocument(doc, sourceUri, inputs.getDocumentClassification());
        }
        String uri = sourceUri + "-extracted-text.json";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new JacksonHandle(doc));
    }

    private DocumentWriteOperation buildExtractedXmlDocument(String sourceUri, DocumentInputs inputs, DocumentMetadataHandle sourceMetadata) {
        Document doc = domHelper.newDocument();
        Element root = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);

        if (!extractedTextConfig.dropSource) {
            Element sourceElement = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "source-uri");
            sourceElement.appendChild(doc.createTextNode(sourceUri));
            root.appendChild(sourceElement);
        }

        Element content = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "content");
        content.appendChild(doc.createTextNode(inputs.getExtractedText()));
        root.appendChild(content);

        if (inputs.getExtractedMetadata() != null && !inputs.getExtractedMetadata().isEmpty()) {
            Element metadata = doc.createElementNS(Util.DEFAULT_XML_NAMESPACE, "extracted-metadata");
            root.appendChild(metadata);
            inputs.getExtractedMetadata().entrySet().forEach(entry -> {
                Element metadataElement = createXmlMetadataElement(doc, entry);
                metadata.appendChild(metadataElement);
            });
        }

        if (inputs.getDocumentClassification() != null) {
            addClassificationToXmlDocument(doc, sourceUri, inputs.getDocumentClassification());
        }
        String uri = sourceUri + "-extracted-text.xml";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new DOMHandle(doc));
    }

    private Element createXmlMetadataElement(Document doc, Map.Entry<String, String> metadataEntry) {
        final String key = metadataEntry.getKey();

        // Ideally, the metadata entry has a recognized XML prefix that can be mapped to a useful namespace.
        // If not, we default to this local name to avoid creating an element with a colon in its name, which is not
        // allowed.
        String localName = metadataEntry.getKey().replace(":", "-");
        String namespace = Util.DEFAULT_XML_NAMESPACE;
        final int pos = key.indexOf(":");
        if (pos > -1) {
            final String prefix = key.substring(0, pos);
            String associatedNamespace = ExtractionUtil.getNamespace(prefix);
            if (associatedNamespace != null) {
                namespace = associatedNamespace;
                localName = key.substring(pos + 1);
            }
        }

        Element metadataElement = doc.createElementNS(namespace, localName);
        metadataElement.appendChild(doc.createTextNode(metadataEntry.getValue()));
        return metadataElement;
    }

    private List<DocumentWriteOperation> buildChunkDocuments(DocumentInputs inputs, DocumentWriteOperation mainDocument, DocumentWriteOperation extractedTextDocument) {
        List<DocumentWriteOperation> chunkDocuments = new ArrayList<>();
        if (inputs.getChunks() != null && !inputs.getChunks().isEmpty()) {
            // If there's an extracted doc, we want to use that as the source document so that the user has the option
            // of adding chunks to it.
            DocumentWriteOperation sourceDocument = extractedTextDocument != null ? extractedTextDocument : mainDocument;
            Iterator<DocumentWriteOperation> iterator = chunkAssembler.assembleChunks(
                sourceDocument,
                inputs.getChunks(),
                inputs.getClassifications(),
                inputs.getEmbeddings());
            while (iterator.hasNext()) {
                DocumentWriteOperation doc = iterator.next();
                chunkDocuments.add(doc);
            }
        }
        return chunkDocuments;
    }
}
