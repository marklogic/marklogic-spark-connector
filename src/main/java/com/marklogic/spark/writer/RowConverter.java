/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Iterator;
import java.util.List;

/**
 * Strategy interface for how a Spark row is converted into a set of inputs for writing a document to MarkLogic.
 */
public interface RowConverter {

    /**
     * @param row
     * @return an iterator of inputs for creating documents to write to MarkLogic. An iterator is used to allow the
     * implementor to return multiple documents if necessary.
     */
    Iterator<DocBuilder.DocumentInputs> convertRow(InternalRow row);

    /**
     * Called when {@code WriteBatcherDataWriter} has no more rows to send, but the implementation may have one or
     * more documents that haven't been returned yet via {@code convertRow}.
     *
     * @return
     */
    List<DocBuilder.DocumentInputs> getRemainingDocumentInputs();
}
