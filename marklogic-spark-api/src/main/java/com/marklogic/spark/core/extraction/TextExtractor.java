/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.spark.core.DocumentInputs;

public interface TextExtractor {

    ExtractionResult extractText(DocumentInputs inputs);
}
