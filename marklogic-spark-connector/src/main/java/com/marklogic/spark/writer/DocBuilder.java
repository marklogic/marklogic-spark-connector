/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.langchain4j.dom.DOMHelper;
import com.marklogic.spark.langchain4j.NamespaceContextFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to build instances of {@code DocumentWriteOperation} based on the data in {@code DocBuilder.DocumentInputs}.
 * The latter is expected to contain data read from a Spark row, normalized into a standard set of inputs regardless
 * of the schema of the Spark row.
 */
public class DocBuilder {

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
    }

    private final UriMaker uriMaker;
    private final DocumentMetadataHandle metadataFromOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DOMHelper domHelper = new DOMHelper(NamespaceContextFactory.makeDefaultNamespaceContext());
    private final Format extractedTextFormat;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadataFromOptions, Format extractedTextFormat) {
        this.uriMaker = uriMaker;
        this.metadataFromOptions = metadataFromOptions;
        this.extractedTextFormat = extractedTextFormat;
    }

    /**
     * @param inputs set of inputs constructed from a single Spark row.
     * @return one or more documents to write to MarkLogic, based on the inputs object.
     */
    List<DocumentWriteOperation> buildDocuments(DocumentInputs inputs) {
        final List<DocumentWriteOperation> documents = new ArrayList<>();
        final String sourceUri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        final String graph = inputs.getGraph();
        final DocumentMetadataHandle metadataFromRow = inputs.getInitialMetadata();

        DocumentMetadataHandle mainDocumentMetadata = metadataFromRow;
        if (mainDocumentMetadata != null) {
            // If the row contains metadata, use it, but first override it based on the metadata specified by user options.
            overrideMetadataFromRowWithMetadataFromOptions(mainDocumentMetadata);
            if (graph != null) {
                mainDocumentMetadata.getCollections().add(graph);
            }
            documents.add(new DocumentWriteOperationImpl(sourceUri, mainDocumentMetadata, inputs.getContent()));
        } else {
            // If the row doesn't contain metadata, use the metadata specified by user options. We need to be careful
            // not to modify that object though, as it will be reused on subsequent calls.
            mainDocumentMetadata = metadataFromOptions;
            if (graph != null && !mainDocumentMetadata.getCollections().contains(graph)) {
                mainDocumentMetadata = newMetadataWithGraph(graph);
            }
            documents.add(new DocumentWriteOperationImpl(sourceUri, mainDocumentMetadata, inputs.getContent()));
        }

        if (inputs.getExtractedText() != null) {
            // For now, just reuse the main metadata. We'll make this configurable soon.
            DocumentWriteOperation extractedTextDoc = Format.XML.equals(this.extractedTextFormat) ?
                buildExtractedXmlDocument(sourceUri, inputs.getExtractedText(), mainDocumentMetadata) :
                buildExtractedJsonDocument(sourceUri, inputs.getExtractedText(), mainDocumentMetadata);
            documents.add(extractedTextDoc);
        }
        return documents;
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

    private DocumentWriteOperation buildExtractedJsonDocument(String sourceUri, String extractedText, DocumentMetadataHandle mainDocumentMetadata) {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.put("content", extractedText);
        String uri = sourceUri + "-extracted-text.json";
        return new DocumentWriteOperationImpl(uri, mainDocumentMetadata, new JacksonHandle(doc));
    }

    private DocumentWriteOperation buildExtractedXmlDocument(String sourceUri, String extractedText, DocumentMetadataHandle mainDocumentMetadata) {
        Document doc = domHelper.newDocument();
        Element root = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);
        Element content = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "content");
        content.appendChild(doc.createTextNode(extractedText));
        root.appendChild(content);
        String uri = sourceUri + "-extracted-text.xml";
        return new DocumentWriteOperationImpl(uri, mainDocumentMetadata, new DOMHandle(doc));
    }
}
