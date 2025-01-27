/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.langchain4j.Util;

/**
 * This is intended to migrate to java-client-api and likely just be a Builder class on DocumentWriteOperation.
 */
class DocBuilderFactory {

    private DocumentMetadataHandle metadataFromOptions;
    private DocBuilder.UriMaker uriMaker;
    private Format extractedTextFormat;

    DocBuilderFactory() {
        this.metadataFromOptions = new DocumentMetadataHandle();
    }

    DocBuilder newDocBuilder() {
        return new DocBuilder(uriMaker, metadataFromOptions, extractedTextFormat);
    }

    DocBuilderFactory withCollections(String collections) {
        if (collections != null && collections.trim().length() > 0) {
            metadataFromOptions.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        Util.addPermissionsFromDelimitedString(metadataFromOptions.getPermissions(), permissionsString);
        return this;
    }

    DocBuilderFactory withUriMaker(DocBuilder.UriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }

    DocBuilderFactory withExtractedTextFormat(String extractedTextFormat) {
        if (extractedTextFormat != null && extractedTextFormat.trim().length() > 0) {
            this.extractedTextFormat = "xml".equalsIgnoreCase(extractedTextFormat) ? Format.XML : Format.JSON;
        }
        return this;
    }
}
