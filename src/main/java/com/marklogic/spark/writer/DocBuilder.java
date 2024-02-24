/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

public class DocBuilder {

    public interface UriMaker {
        String makeURI(String initialUri, ObjectNode columnValues);
    }

    /**
     * Captures the various inputs used for constructing a document to be written to MarkLogic. {@code graph} refers
     * to an optional MarkLogic semantics graph, which must be added to the final set of collections for the
     * document.
     */
    public static class DocumentInputs {
        private final String initialUri;
        private final AbstractWriteHandle content;
        private final ObjectNode columnValuesForUriTemplate;
        private final DocumentMetadataHandle initialMetadata;
        private final String graph;

        public DocumentInputs(String initialUri, AbstractWriteHandle content, ObjectNode columnValuesForUriTemplate,
                              DocumentMetadataHandle initialMetadata) {
            this(initialUri, content, columnValuesForUriTemplate, initialMetadata, null);
        }

        public DocumentInputs(String initialUri, AbstractWriteHandle content, ObjectNode columnValuesForUriTemplate,
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

        ObjectNode getColumnValuesForUriTemplate() {
            return columnValuesForUriTemplate;
        }

        DocumentMetadataHandle getInitialMetadata() {
            return initialMetadata;
        }

        String getGraph() {
            return graph;
        }
    }

    private final UriMaker uriMaker;
    private final DocumentMetadataHandle metadata;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadata) {
        this.uriMaker = uriMaker;
        this.metadata = metadata;
    }

    DocumentWriteOperation build(DocumentInputs inputs) {
        final String uri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        final String graph = inputs.getGraph();
        final DocumentMetadataHandle initialMetadata = inputs.getInitialMetadata();

        if (initialMetadata != null) {
            overrideInitialMetadata(initialMetadata);
            if (graph != null) {
                initialMetadata.getCollections().add(graph);
            }
            return new DocumentWriteOperationImpl(uri, initialMetadata, inputs.getContent());
        }

        if (graph != null && !metadata.getCollections().contains(graph)) {
            DocumentMetadataHandle newMetadata = newMetadataWithGraph(graph);
            return new DocumentWriteOperationImpl(uri, newMetadata, inputs.getContent());
        }

        return new DocumentWriteOperationImpl(uri, metadata, inputs.getContent());
    }

    /**
     * If an instance of {@code DocumentInputs} has metadata specified, override it with anything in the metadata
     * instance that was used to construct this class. We could later support an additive approach as well here.
     *
     * @param initialMetadata
     */
    private void overrideInitialMetadata(DocumentMetadataHandle initialMetadata) {
        if (!metadata.getCollections().isEmpty()) {
            initialMetadata.setCollections(metadata.getCollections());
        }
        if (!metadata.getPermissions().isEmpty()) {
            initialMetadata.setPermissions(metadata.getPermissions());
        }
        if (metadata.getQuality() != 0) {
            initialMetadata.setQuality(metadata.getQuality());
        }
        if (!metadata.getProperties().isEmpty()) {
            initialMetadata.setProperties(metadata.getProperties());
        }
        if (!metadata.getMetadataValues().isEmpty()) {
            initialMetadata.setMetadataValues(metadata.getMetadataValues());
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
        newMetadata.getCollections().addAll(metadata.getCollections());
        newMetadata.getPermissions().putAll(metadata.getPermissions());
        newMetadata.setQuality(metadata.getQuality());
        newMetadata.setProperties(metadata.getProperties());
        newMetadata.setMetadataValues(metadata.getMetadataValues());
        newMetadata.getCollections().add(graph);
        return newMetadata;
    }
}
