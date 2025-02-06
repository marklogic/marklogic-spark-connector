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
import com.marklogic.langchain4j.splitter.ChunkAssembler;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.dom.DOMHelper;
import com.marklogic.spark.langchain4j.NamespaceContextFactory;
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

    private static final String CLASSIFICATION_MAIN_ELEMENT = "STRUCTUREDDOCUMENT";

    public interface UriMaker {
        String makeURI(String initialUri, JsonNode uriTemplateValues);
    }

    /**
     * Captures the various inputs used for constructing a document to be written to MarkLogic. {@code graph} refers
     * to an optional MarkLogic semantics graph, which must be added to the final set of collections for the
     * document.
     */
    public static class DocumentInputs {
        private final String initialUri;
        private final AbstractWriteHandle content;
        private final JsonNode columnValuesForUriTemplate;
        private final DocumentMetadataHandle initialMetadata;
        private final String graph;

        // Using hacky getter/setter for now until this is proven to work well.
        private String extractedText;
        private List<String> chunks;
        private List<byte[]> classifications;
        private byte[] classificationResponse;

        public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                              DocumentMetadataHandle initialMetadata) {
            this(initialUri, content, columnValuesForUriTemplate, initialMetadata, null);
        }

        public DocumentInputs(String initialUri, AbstractWriteHandle content, JsonNode columnValuesForUriTemplate,
                              DocumentMetadataHandle initialMetadata, String graph) {
            this.initialUri = initialUri;
            this.content = content;
            this.columnValuesForUriTemplate = columnValuesForUriTemplate;
            this.initialMetadata = initialMetadata;
            this.graph = graph;
        }

        String getInitialUri() {
            return initialUri;
        }

        AbstractWriteHandle getContent() {
            return content;
        }

        JsonNode getColumnValuesForUriTemplate() {
            return columnValuesForUriTemplate;
        }

        DocumentMetadataHandle getInitialMetadata() {
            return initialMetadata;
        }

        String getGraph() {
            return graph;
        }

        public String getExtractedText() {
            return extractedText;
        }

        public void setExtractedText(String extractedText) {
            this.extractedText = extractedText;
        }

        public List<String> getChunks() {
            return chunks;
        }

        public void setChunks(List<String> chunks) {
            this.chunks = chunks;
        }

        public List<byte[]> getClassifications() {
            return classifications;
        }

        public void setClassifications(List<byte[]> classifications) {
            this.classifications = classifications;
        }

        public byte[] getClassificationResponse() {
            return classificationResponse;
        }

        public void setClassificationResponse(byte[] classificationResponse) {
            this.classificationResponse = classificationResponse;
        }
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
    private DocumentBuilderFactory documentBuilderFactory = null;
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
        Format sourceDocumentFormat = com.marklogic.spark.Util.determineSourceDocumentFormat(content, sourceUri);

        byte[] classificationResponse = inputs.getClassificationResponse();
        if (classificationResponse != null && inputs.getExtractedText() == null) {
            if (Format.XML.equals(sourceDocumentFormat)) {
                Document originalDoc = domHelper.extractDocument(content, inputs.getInitialUri());
                content = appendDocumentClassificationToXmlContent(originalDoc, inputs.getInitialUri(), classificationResponse);
            } else if (Format.JSON.equals(sourceDocumentFormat)) {
                content = appendDocumentClassificationToJsonContent(com.marklogic.spark.Util.getJsonFromHandle(content), inputs.getInitialUri(), classificationResponse);
            } else {
                if (sourceDocumentFormat == null) {
                    Util.MAIN_LOGGER.warn("Cannot add classification to document with URI {}; document is neither JSON nor XML.", sourceUri);
                }
            }
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

    private JacksonHandle appendDocumentClassificationToJsonContent(
        JsonNode originalJsonContent, String uri, byte[] classificationResponse
    ) {
        try {
            JsonNode structuredDocumentNode = xmlMapper.readTree(classificationResponse).get(CLASSIFICATION_MAIN_ELEMENT);
            ((ObjectNode) originalJsonContent).set("classification", structuredDocumentNode);
            return new JacksonHandle(originalJsonContent);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    public DOMHandle appendDocumentClassificationToXmlContent(Document originalDoc, String uri, byte[] classificationResponse) {
        if (documentBuilderFactory == null) {
            documentBuilderFactory = DocumentBuilderFactory.newInstance();
        }
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document responseDoc = builder.parse(new ByteArrayInputStream(classificationResponse));

            NodeList structuredDocumentNodeChildNodes = responseDoc.getElementsByTagName(CLASSIFICATION_MAIN_ELEMENT).item(0).getChildNodes();
            Node classificationNode = originalDoc.createElementNS(com.marklogic.spark.Util.DEFAULT_XML_NAMESPACE, "classification");
            for (int i = 0; i < structuredDocumentNodeChildNodes.getLength(); i++) {
                Node importedChildNode = originalDoc.importNode(structuredDocumentNodeChildNodes.item(i), true);
                classificationNode.appendChild(importedChildNode);
            }
            originalDoc.getFirstChild().appendChild(classificationNode);

            return new DOMHandle(originalDoc);
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
                buildExtractedXmlDocument(sourceUri, inputs.getExtractedText(), metadataToUse) :
                buildExtractedJsonDocument(sourceUri, inputs.getExtractedText(), metadataToUse);
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

    private DocumentWriteOperation buildExtractedJsonDocument(String sourceUri, String extractedText, DocumentMetadataHandle sourceMetadata) {
        ObjectNode doc = objectMapper.createObjectNode();
        if (!extractedTextConfig.dropSource) {
            doc.put("source-uri", sourceUri);
        }
        doc.put("content", extractedText);
        String uri = sourceUri + "-extracted-text.json";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new JacksonHandle(doc));
    }

    private DocumentWriteOperation buildExtractedXmlDocument(String sourceUri, String extractedText, DocumentMetadataHandle sourceMetadata) {
        Document doc = domHelper.newDocument();

        Element root = doc.createElementNS(com.marklogic.spark.Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);

        if (!extractedTextConfig.dropSource) {
            Element sourceElement = doc.createElementNS(com.marklogic.spark.Util.DEFAULT_XML_NAMESPACE, "source-uri");
            sourceElement.appendChild(doc.createTextNode(sourceUri));
            root.appendChild(sourceElement);
        }

        Element content = doc.createElementNS(com.marklogic.spark.Util.DEFAULT_XML_NAMESPACE, "content");
        content.appendChild(doc.createTextNode(extractedText));
        root.appendChild(content);

        String uri = sourceUri + "-extracted-text.xml";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new DOMHandle(doc));
    }

    private List<DocumentWriteOperation> buildChunkDocuments(DocumentInputs inputs, DocumentWriteOperation mainDocument, DocumentWriteOperation extractedTextDocument) {
        List<DocumentWriteOperation> chunkDocuments = new ArrayList<>();
        if (inputs.getChunks() != null && !inputs.getChunks().isEmpty()) {
            // If there's an extracted doc, we want to use that as the source document so that the user has the option
            // of adding chunks to it.
            DocumentWriteOperation sourceDocument = extractedTextDocument != null ? extractedTextDocument : mainDocument;
            Iterator<DocumentWriteOperation> iterator = chunkAssembler.assembleStringChunks(sourceDocument, inputs.getChunks(), inputs.getClassifications());
            while (iterator.hasNext()) {
                chunkDocuments.add(iterator.next());
            }
        }
        return chunkDocuments;
    }
}
