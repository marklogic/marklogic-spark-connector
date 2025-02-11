/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import org.apache.tika.Tika;

public interface NewDocumentProcessorFactory {

    static NewDocumentProcessor newDocumentProcessor(Context context) {
        if (context.getBooleanOption(Options.WRITE_EXTRACTED_TEXT, false)) {
            return new NewDocumentProcessor(new Tika());
        }
        return null;
    }

}
