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
import com.marklogic.langchain4j.splitter.ChunkAssembler;
import com.marklogic.spark.langchain4j.NamespaceContextFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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
    }

    private final UriMaker uriMaker;
    private final DocumentMetadataHandle metadataFromOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DOMHelper domHelper = new DOMHelper(NamespaceContextFactory.makeDefaultNamespaceContext());
    private final Format extractedTextFormat;
    private final ChunkAssembler chunkAssembler;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadataFromOptions, Format extractedTextFormat, ChunkAssembler chunkAssembler) {
        this.uriMaker = uriMaker;
        this.metadataFromOptions = metadataFromOptions;
        this.extractedTextFormat = extractedTextFormat;
        this.chunkAssembler = chunkAssembler;
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
        return documents.values();
    }

    private DocumentWriteOperation buildMainDocument(DocumentInputs inputs) {
        final String sourceUri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        final String graph = inputs.getGraph();
        final DocumentMetadataHandle metadataFromRow = inputs.getInitialMetadata();

        DocumentMetadataHandle sourceMetadata = metadataFromRow;
        if (sourceMetadata != null) {
            // If the row contains metadata, use it, but first override it based on the metadata specified by user options.
            overrideMetadataFromRowWithMetadataFromOptions(sourceMetadata);
            if (graph != null) {
                sourceMetadata.getCollections().add(graph);
            }
            return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, inputs.getContent());
        }
        // If the row doesn't contain metadata, use the metadata specified by user options. We need to be careful
        // not to modify that object though, as it will be reused on subsequent calls.
        sourceMetadata = metadataFromOptions;
        if (graph != null && !sourceMetadata.getCollections().contains(graph)) {
            sourceMetadata = newMetadataWithGraph(graph);
        }
        return new DocumentWriteOperationImpl(sourceUri, sourceMetadata, inputs.getContent());
    }

    private DocumentWriteOperation buildExtractedTextDocument(DocumentInputs inputs, DocumentWriteOperation mainDocument) {
        if (inputs.getExtractedText() != null) {
            String sourceUri = mainDocument.getUri();
            // For now, just reuse the main document metadata. We'll make this configurable soon.
            DocumentMetadataHandle sourceMetadata = (DocumentMetadataHandle) mainDocument.getMetadata();
            return Format.XML.equals(this.extractedTextFormat) ?
                buildExtractedXmlDocument(sourceUri, inputs.getExtractedText(), sourceMetadata) :
                buildExtractedJsonDocument(sourceUri, inputs.getExtractedText(), sourceMetadata);
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
        doc.put("source-uri", sourceUri);
        doc.put("content", extractedText);
        String uri = sourceUri + "-extracted-text.json";
        return new DocumentWriteOperationImpl(uri, sourceMetadata, new JacksonHandle(doc));
    }

    private DocumentWriteOperation buildExtractedXmlDocument(String sourceUri, String extractedText, DocumentMetadataHandle sourceMetadata) {
        Document doc = domHelper.newDocument();

        Element root = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);

        Element sourceElement = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "source-uri");
        sourceElement.appendChild(doc.createTextNode(sourceUri));
        root.appendChild(sourceElement);

        Element content = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "content");
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
            Iterator<DocumentWriteOperation> iterator = chunkAssembler.assembleStringChunks(sourceDocument, inputs.getChunks());
            while (iterator.hasNext()) {
                chunkDocuments.add(iterator.next());
            }
        }
        return chunkDocuments;
    }
}
