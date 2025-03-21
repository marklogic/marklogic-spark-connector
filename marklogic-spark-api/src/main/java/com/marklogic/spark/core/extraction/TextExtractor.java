/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.spark.core.DocumentInputs;

import java.util.Optional;

public interface TextExtractor {

    Optional<ExtractionResult> extractText(DocumentInputs inputs);
}
