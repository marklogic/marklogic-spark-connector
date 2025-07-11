/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.embedding;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.dom.NamespaceContextFactory;

public interface ChunkSelectorFactory {

    static ChunkSelector makeChunkSelector(Context context) {
        if (context.getProperties().get(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER) != null) {
            // "" is allowed for the chunks JSON pointer.
            return makeJsonChunkSelector(context);
        } else if (context.hasOption(Options.WRITE_EMBEDDER_CHUNKS_XPATH)) {
            return makeXmlChunkSelector(context);
        }
        return null;
    }

    private static ChunkSelector makeJsonChunkSelector(Context context) {
        return new JsonChunkSelector.Builder()
            .withChunksPointer(context.getProperties().get(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER))
            .withTextPointer(context.getStringOption(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER))
            .withEmbeddingArrayName(context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAME))
            .withBase64EncodeVectors(context.getBooleanOption(Options.WRITE_EMBEDDER_BASE64_ENCODE, false))
            .build();
    }

    private static ChunkSelector makeXmlChunkSelector(Context context) {
        XmlChunkConfig xmlChunkConfig = new XmlChunkConfig(
            context.getStringOption(Options.WRITE_EMBEDDER_TEXT_XPATH),
            context.getStringOption(Options.WRITE_EMBEDDER_EMBEDDING_NAME),
            context.getProperties().get(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE),
            NamespaceContextFactory.makeNamespaceContext(context.getProperties()),
            context.getBooleanOption(Options.WRITE_EMBEDDER_BASE64_ENCODE, false)
        );
        return new DOMChunkSelector(
            context.getStringOption(Options.WRITE_EMBEDDER_CHUNKS_XPATH),
            xmlChunkConfig
        );
    }
}
