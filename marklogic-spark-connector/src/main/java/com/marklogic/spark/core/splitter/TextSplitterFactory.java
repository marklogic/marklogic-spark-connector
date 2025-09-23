/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.splitter;

import com.marklogic.spark.Context;

public interface TextSplitterFactory {

    TextSplitter newTextSplitter(Context context);
}
