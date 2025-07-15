/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
        // Use the Caddy port to avoid connection failures on Jenkins.
        return 8116;
    }
}
