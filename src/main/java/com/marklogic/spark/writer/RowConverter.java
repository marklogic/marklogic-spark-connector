/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.List;
import java.util.Optional;

/**
 * Strategy interface for how a Spark row is converted into a set of inputs for writing a document to MarkLogic.
 */
public interface RowConverter {

    /**
     * An implementation can return an empty Optional, which will happen when the row will be used with other rows to
     * form a document.
     *
     * @param row
     * @return
     */
    Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row);

    /**
     * Called when {@code WriteBatcherDataWriter} has no more rows to send, but the implementation may have one or
     * more documents that haven't been returned yet via {@code convertRow}.
     *
     * @return
     */
    List<DocBuilder.DocumentInputs> getRemainingDocumentInputs();
}
