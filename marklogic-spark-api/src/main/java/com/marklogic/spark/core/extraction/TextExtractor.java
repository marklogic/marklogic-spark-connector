/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import com.marklogic.spark.core.DocumentInputs;

import java.util.Optional;

public interface TextExtractor {

    Optional<ExtractionResult> extractText(DocumentInputs inputs);
}
