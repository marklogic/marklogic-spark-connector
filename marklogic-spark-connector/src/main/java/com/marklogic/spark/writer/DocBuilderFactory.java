/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.langchain4j.Util;
import com.marklogic.langchain4j.splitter.ChunkAssembler;

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
        if (collections != null && collections.trim().length() > 0) {
            metadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        Util.addPermissionsFromDelimitedString(metadata.getPermissions(), permissionsString);
        return this;
    }

    DocBuilderFactory withUriMaker(DocBuilder.UriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }

    DocBuilderFactory withExtractedTextDocumentType(String extractedTextDocumentType) {
        if (extractedTextDocumentType != null && extractedTextDocumentType.trim().length() > 0) {
            this.extractedTextFormat = "xml".equalsIgnoreCase(extractedTextDocumentType) ? Format.XML : Format.JSON;
        }
        return this;
    }

    DocBuilderFactory withExtractedTextCollections(String collections) {
        if (collections != null && collections.trim().length() > 0) {
            if (extractedTextMetadata == null) {
                extractedTextMetadata = new DocumentMetadataHandle();
            }
            extractedTextMetadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withExtractedTextPermissions(String permissionsString) {
        if (permissionsString != null && permissionsString.trim().length() > 0) {
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
