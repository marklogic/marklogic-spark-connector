/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.Iterator;

public interface ChunkSelector {

    Iterator<Chunk> selectChunks(DocumentWriteOperation sourceDocument);
}
