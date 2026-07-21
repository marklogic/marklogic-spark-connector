/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.junit5.spring.SimpleTestConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(
    value = {"file:../test-app/gradle.properties", "file:../test-app/gradle-local.properties"},
    ignoreResourceNotFound = true
)
public class TestConfig extends SimpleTestConfig {

    @Override
    public Integer getRestPort() {
        // Allow local runs to override the default Caddy port when a direct MarkLogic port is healthier.
        String restPort = System.getProperty("mlRestPort");
        if (restPort == null || restPort.isBlank()) {
            restPort = System.getenv("ML_REST_PORT");
        }
        return restPort != null && !restPort.isBlank() ? Integer.parseInt(restPort) : 8116;
    }
}
