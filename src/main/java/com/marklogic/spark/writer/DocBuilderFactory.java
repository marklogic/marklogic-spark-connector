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

import com.marklogic.client.io.DocumentMetadataHandle;

import java.util.UUID;

/**
 * This is intended to migrate to java-client-api and likely just be a Builder class on DocumentWriteOperation.
 */
class DocBuilderFactory {

    private DocumentMetadataHandle metadata;
    private DocBuilder.UriMaker uriMaker;

    DocBuilderFactory() {
        this.metadata = new DocumentMetadataHandle();
    }

    DocBuilder newDocBuilder() {
        return new DocBuilder(uriMaker, metadata);
    }

    DocBuilderFactory withCollections(String collections) {
        if (collections != null && collections.trim().length() > 0) {
            metadata.withCollections(collections.split(","));
        }
        return this;
    }

    DocBuilderFactory withPermissions(String permissionsString) {
        metadata.getPermissions().addFromDelimitedString(permissionsString);
        return this;
    }

    DocBuilderFactory withSimpleUriStrategy(String prefix, String suffix) {
        return withUriMaker((initialUri, columnValues) -> {
            String uri = initialUri != null ? initialUri : "";
            if (prefix != null) {
                uri = prefix + uri;
            }
            if (initialUri == null || initialUri.trim().length() == 0) {
                uri += UUID.randomUUID().toString();
            }
            return suffix != null ? uri + suffix : uri;
        });
    }

    DocBuilderFactory withUriMaker(DocBuilder.UriMaker uriMaker) {
        this.uriMaker = uriMaker;
        return this;
    }
}
