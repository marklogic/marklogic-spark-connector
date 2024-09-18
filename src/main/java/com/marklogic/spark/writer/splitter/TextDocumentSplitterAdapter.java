/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.spark.Options;
import com.marklogic.spark.writer.WriteContext;
import dev.langchain4j.data.document.splitter.DocumentSplitters;

public class TextDocumentSplitterAdapter extends TextDocumentSplitter {

    public TextDocumentSplitterAdapter(WriteContext context) {
        super(
            DocumentSplitters.recursive(
                (int) context.getNumericOption(Options.WRITE_DOCUMENT_SPLITTER_MAX_SEGMENT_SIZE, 1000, 0),
                (int) context.getNumericOption(Options.WRITE_DOCUMENT_SPLITTER_MAX_OVERLAP, 0, 0)
            )
        );
    }
}
