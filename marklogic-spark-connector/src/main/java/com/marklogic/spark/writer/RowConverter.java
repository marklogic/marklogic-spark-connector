/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.core.DocumentInputs;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Iterator;

/**
 * Strategy interface for how a Spark row is converted into a set of inputs for writing a document to MarkLogic.
 */
public interface RowConverter {

    /**
     * @param row
     * @return an iterator of inputs for creating documents to write to MarkLogic. An iterator is used to allow the
     * implementor to return multiple documents if necessary.
     */
    Iterator<DocumentInputs> convertRow(InternalRow row);

    /**
     * Called when {@code WriteBatcherDataWriter} has no more rows to send, but the implementation may have one or
     * more documents that haven't been returned yet via {@code convertRow}.
     *
     * @return
     */
    Iterator<DocumentInputs> getRemainingDocumentInputs();
}
