/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.extraction;

import java.util.Map;

public class ExtractionResult {

    private final String text;
    private final Map<String, String> metadata;

    public ExtractionResult(String text, Map<String, String> metadata) {
        this.text = text;
        this.metadata = metadata;
    }

    public String getText() {
        return text;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
