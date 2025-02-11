/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.splitter.TextSplitter;
import org.apache.tika.Tika;

public interface NewDocumentProcessorFactory {

    static NewDocumentProcessor newDocumentProcessor(Context context) {
        final Tika tika = context.getBooleanOption(Options.WRITE_EXTRACTED_TEXT, false) ?
            new Tika() : null;
        final TextSplitter textSplitter = DocumentProcessorFactory.newTextSplitter(context);
        return new NewDocumentProcessor(tika, textSplitter);
    }

}
