/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.nuclia;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * For manual testing of Nuclia integration.
 */
class NucliaAdHocTest extends AbstractIntegrationTest {

    @EnabledIfEnvironmentVariable(
        named = "NUCLIA_NUA_KEY", matches = ".*"
    )
    @Test
    void nuclia() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/armstrong_neil.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_NUCLIA_NUA_KEY, System.getenv("NUCLIA_NUA_KEY"))
            .option(Options.WRITE_URI_REPLACE, ".*extraction-files,'/aaa'")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();
    }
}
