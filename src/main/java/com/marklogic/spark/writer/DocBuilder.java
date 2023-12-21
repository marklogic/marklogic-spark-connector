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

class DocBuilder {

    public interface UriMaker {
        String makeURI(String initialUri, ObjectNode columnValues);
    }

    public static class DocumentInputs {
        private String initialUri;
        private AbstractWriteHandle content;
        private ObjectNode columnValuesForUriTemplate;

        public DocumentInputs(String initialUri, AbstractWriteHandle content, ObjectNode columnValuesForUriTemplate) {
            this.initialUri = initialUri;
            this.content = content;
            this.columnValuesForUriTemplate = columnValuesForUriTemplate;
        }

        public String getInitialUri() {
            return initialUri;
        }

        public AbstractWriteHandle getContent() {
            return content;
        }

        public ObjectNode getColumnValuesForUriTemplate() {
            return columnValuesForUriTemplate;
        }
    }

    private final UriMaker uriMaker;
    private DocumentMetadataHandle metadata;

    DocBuilder(UriMaker uriMaker, DocumentMetadataHandle metadata) {
        this.uriMaker = uriMaker;
        this.metadata = metadata;
    }

    DocumentWriteOperation build(DocumentInputs inputs) {
        String uri = uriMaker.makeURI(inputs.getInitialUri(), inputs.getColumnValuesForUriTemplate());
        return new DocumentWriteOperationImpl(uri, metadata, inputs.getContent());
    }
}
