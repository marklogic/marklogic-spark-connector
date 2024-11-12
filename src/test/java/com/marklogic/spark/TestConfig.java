/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.junit5.spring.SimpleTestConfig;

public class TestConfig extends SimpleTestConfig {

    @Override
    public Integer getRestPort() {
        // Use the Caddy port to avoid connection failures on Jenkins.
        return 8116;
    }
}
