/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.spark.Context;

public interface TextSplitterFactory {

    TextSplitter newTextSplitter(Context context);
}
