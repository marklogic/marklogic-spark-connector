/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.langchain4j.document.splitter.JsonDocumentSplitter;
import com.marklogic.spark.Options;
import dev.langchain4j.data.document.splitter.DocumentSplitters;

/**
 * Extends {@code JsonDocumentSplitter} so it can be constructed based on Spark options.
 */
class JsonDocumentSplitterAdapter extends JsonDocumentSplitter {

    JsonDocumentSplitterAdapter(WriteContext context) {
        super(
            DocumentSplitters.recursive(
                (int) context.getNumericOption(Options.WRITE_DOCUMENT_SPLITTER_MAX_SEGMENT_SIZE, 1000, 0),
                (int) context.getNumericOption(Options.WRITE_DOCUMENT_SPLITTER_MAX_OVERLAP, 0, 0)
            ),
            context.getStringOption(Options.WRITE_DOCUMENT_SPLITTER_FIELD_NAME)
        );
    }
}
