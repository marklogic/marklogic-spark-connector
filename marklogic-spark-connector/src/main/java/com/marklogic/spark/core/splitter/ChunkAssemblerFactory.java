/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;

public interface ChunkAssemblerFactory {

    static ChunkAssembler makeChunkAssembler(Context context) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();

        if (context.hasOption(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS)) {
            metadata.getCollections().addAll(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS).split(","));
        }

        if (context.hasOption(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS);
            Util.addPermissionsFromDelimitedString(metadata.getPermissions(), value);
        } else if (context.hasOption(Options.WRITE_PERMISSIONS)) {
            String value = context.getStringOption(Options.WRITE_PERMISSIONS);
            Util.addPermissionsFromDelimitedString(metadata.getPermissions(), value);
        }

        return new DefaultChunkAssembler(new ChunkConfig.Builder()
            .withMetadata(metadata)
            .withMaxChunks(context.getIntOption(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 0, 0))
            .withDocumentType(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE))
            .withRootName(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME))
            .withUriPrefix(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_URI_PREFIX))
            .withUriSuffix(context.getStringOption(Options.WRITE_SPLITTER_SIDECAR_URI_SUFFIX))
            .withXmlNamespace(context.getProperties().get(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE))
            .withEmbeddingXmlNamespace(context.getProperties().get(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE))
            .withBase64EncodeVectors(context.getBooleanOption(Options.WRITE_EMBEDDER_BASE64_ENCODE, false))
            .build()
        );
    }
}
