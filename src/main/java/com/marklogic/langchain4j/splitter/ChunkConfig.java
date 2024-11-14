/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.marklogic.client.io.DocumentMetadataHandle;

public class ChunkConfig {

    private final DocumentMetadataHandle metadata;
    private final int maxChunks;
    private final String documentType;
    private final String rootName;
    private final String xmlNamespace;
    private final String uriPrefix;
    private final String uriSuffix;

    private ChunkConfig(DocumentMetadataHandle metadata, int maxChunks, String documentType, String rootName, String xmlNamespace, String uriPrefix, String uriSuffix) {
        this.metadata = metadata;
        this.maxChunks = maxChunks;
        this.documentType = documentType;
        this.rootName = rootName;
        this.xmlNamespace = xmlNamespace;
        this.uriPrefix = uriPrefix;
        this.uriSuffix = uriSuffix;
    }

    public static class Builder {
        private DocumentMetadataHandle metadata;
        private int maxChunks;
        private String documentType;
        private String rootName;
        private String xmlNamespace;
        private String uriPrefix;
        private String uriSuffix;

        public ChunkConfig build() {
            return new ChunkConfig(metadata, maxChunks, documentType, rootName, xmlNamespace, uriPrefix, uriSuffix);
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
            this.xmlNamespace = xmlNamespace;
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
}
