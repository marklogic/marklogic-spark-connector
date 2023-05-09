package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

/**
 * This is intended to be replaced by something generic in the Java Client which can then be reused in other Java-based
 * connectors that must support a similar use case of taking a set of user options and producing a
 * DocumentWriteOperation.
 */
class DocBuilder {

    private DocumentWriteOperation.DocumentUriMaker uriMaker;
    private DocumentMetadataHandle metadata;


    DocBuilder(DocumentWriteOperation.DocumentUriMaker uriMaker, DocumentMetadataHandle metadata) {
        this.uriMaker = uriMaker;
        this.metadata = metadata;
    }

    DocumentWriteOperation build(AbstractWriteHandle contentHandle) {
        return new DocumentWriteOperationImpl(uriMaker.apply(contentHandle), metadata, contentHandle);
    }
}
