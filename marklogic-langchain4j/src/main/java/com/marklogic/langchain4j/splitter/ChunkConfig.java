/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.Util;

/**
 * Captures configuration settings for producing chunks, either in a source document or in separate
 * sidecar documents.
 */
public class ChunkConfig {

    private final DocumentMetadataHandle metadata;
    private final int maxChunks;
    private final String documentType;
    private final String rootName;
    private final String xmlNamespace;
    private final String embeddingXmlNamespace;
    private final String uriPrefix;
    private final String uriSuffix;

    // Ignoring Sonar warning about too many constructor args, as that's mitigated via the builder.
    @SuppressWarnings("java:S107")
    private ChunkConfig(DocumentMetadataHandle metadata, int maxChunks, String documentType, String rootName,
                        String xmlNamespace, String embeddingXmlNamespace, String uriPrefix, String uriSuffix) {
        this.metadata = metadata;
        this.maxChunks = maxChunks;
        this.documentType = documentType;
        this.rootName = rootName;
        this.xmlNamespace = xmlNamespace;
        this.embeddingXmlNamespace = embeddingXmlNamespace;
        this.uriPrefix = uriPrefix;
        this.uriSuffix = uriSuffix;
    }

    public static class Builder {
        private DocumentMetadataHandle metadata;
        private int maxChunks;
        private String documentType;
        private String rootName;
        private String xmlNamespace = Util.DEFAULT_XML_NAMESPACE;
        private String embeddingXmlNamespace;
        private String uriPrefix;
        private String uriSuffix;

        public ChunkConfig build() {
            String tempNamespace = embeddingXmlNamespace;
            if (tempNamespace == null) {
                // If no embedding XML namespace is specified, default to the chunk namespace is defined.
                tempNamespace = xmlNamespace != null ? xmlNamespace : Util.DEFAULT_XML_NAMESPACE;
            }
            return new ChunkConfig(metadata, maxChunks, documentType, rootName, xmlNamespace, tempNamespace, uriPrefix, uriSuffix);
        }

        public Builder withMetadata(DocumentMetadataHandle metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder withMaxChunks(int maxChunks) {
            this.maxChunks = maxChunks;
            return this;
        }

        public Builder withDocumentType(String documentType) {
            this.documentType = documentType;
            return this;
        }

        public Builder withRootName(String rootName) {
            this.rootName = rootName;
            return this;
        }

        public Builder withXmlNamespace(String xmlNamespace) {
            if (xmlNamespace != null) {
                this.xmlNamespace = xmlNamespace;
            }
            return this;
        }

        public Builder withEmbeddingXmlNamespace(String embeddingXmlNamespace) {
            if (embeddingXmlNamespace != null) {
                this.embeddingXmlNamespace = embeddingXmlNamespace;
            }
            return this;
        }

        public Builder withUriPrefix(String uriPrefix) {
            this.uriPrefix = uriPrefix;
            return this;
        }

        public Builder withUriSuffix(String uriSuffix) {
            this.uriSuffix = uriSuffix;
            return this;
        }
    }

    public DocumentMetadataHandle getMetadata() {
        return metadata;
    }

    public int getMaxChunks() {
        return maxChunks;
    }

    public String getDocumentType() {
        return documentType;
    }

    public String getRootName() {
        return rootName;
    }

    public String getUriPrefix() {
        return uriPrefix;
    }

    public String getUriSuffix() {
        return uriSuffix;
    }

    public String getXmlNamespace() {
        return xmlNamespace;
    }

    public String getEmbeddingXmlNamespace() {
        return embeddingXmlNamespace;
    }
}
