/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.core.splitter.ChunkAssembler;
import com.marklogic.spark.Util;

/**
 * This is intended to migrate to java-client-api and likely just be a Builder class on DocumentWriteOperation.
 */
class DocBuilderFactory {

    private DocumentMetadataHandle metadata;
    private DocumentMetadataHandle extractedTextMetadata;
    private DocBuilder.UriMaker uriMaker;
    private Format extractedTextFormat;
    private boolean extractedTextDropSource;
    private ChunkAssembler chunkAssembler;

    DocBuilderFactory() {
        this.metadata = new DocumentMetadataHandle();
    }

    DocBuilder newDocBuilder() {
        DocBuilder.ExtractedTextConfig extractedTextConfig = new DocBuilder.ExtractedTextConfig(extractedTextFormat,
            extractedTextMetadata, extractedTextDropSource);
        return new DocBuilder(uriMaker, metadata, extractedTextConfig, chunkAssembler);
    }

    DocBuilderFactory withCollections(String collections) {
        if (collections != null && !collections.trim().isEmpty()) {
            metadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        com.marklogic.spark.Util.addPermissionsFromDelimitedString(metadata.getPermissions(), permissionsString);
        return this;
    }

    DocBuilderFactory withMetadataValue(String key, String value) {
        metadata.withMetadataValue(key, value);
        return this;
    }

    DocBuilderFactory withDocumentProperty(String name, Object value) {
        metadata.withProperty(name, value);
        return this;
    }

    DocBuilderFactory withUriMaker(DocBuilder.UriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }

    DocBuilderFactory withExtractedTextDocumentType(String extractedTextDocumentType) {
        if (extractedTextDocumentType != null && !extractedTextDocumentType.trim().isEmpty()) {
            this.extractedTextFormat = "xml".equalsIgnoreCase(extractedTextDocumentType) ? Format.XML : Format.JSON;
        }
        return this;
    }

    DocBuilderFactory withExtractedTextCollections(String collections) {
        if (collections != null && !collections.trim().isEmpty()) {
            if (extractedTextMetadata == null) {
                extractedTextMetadata = new DocumentMetadataHandle();
            }
            extractedTextMetadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withExtractedTextPermissions(String permissionsString) {
        if (permissionsString != null && !permissionsString.trim().isEmpty()) {
            if (extractedTextMetadata == null) {
                extractedTextMetadata = new DocumentMetadataHandle();
            }
            Util.addPermissionsFromDelimitedString(extractedTextMetadata.getPermissions(), permissionsString);
        }
        return this;
    }

    DocBuilderFactory withExtractedTextDropSource(boolean extractedTextDropSource) {
        this.extractedTextDropSource = extractedTextDropSource;
        return this;
    }

    DocBuilderFactory withChunkAssembler(ChunkAssembler chunkAssembler) {
        this.chunkAssembler = chunkAssembler;
        return this;
    }
}
