/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;

/**
 * Captures all the metadata, including document format, from an XML metadata entry in an MLCP archive file.
 */
class MlcpMetadata {

    private DocumentMetadataHandle metadata;
    private Format format;

    MlcpMetadata(DocumentMetadataHandle metadata, Format format) {
        this.metadata = metadata;
        this.format = format;
    }

    DocumentMetadataHandle getMetadata() {
        return metadata;
    }

    Format getFormat() {
        return format;
    }
}
