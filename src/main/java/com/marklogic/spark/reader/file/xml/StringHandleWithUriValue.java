/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file.xml;

import com.marklogic.client.io.StringHandle;

/**
 * Captures a URI value based on the user-defined URI element.
 */
class StringHandleWithUriValue extends StringHandle {

    private final String uriValue;

    StringHandleWithUriValue(String content, String uriValue) {
        super(content);
        this.uriValue = uriValue;
    }

    String getUriValue() {
        return uriValue;
    }
}
