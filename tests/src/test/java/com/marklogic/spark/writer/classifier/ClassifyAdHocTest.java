/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class ClassifyAdHocTest extends AbstractIntegrationTest {

    @EnabledIfEnvironmentVariable(
        named = "SEMAPHORE_API_KEY", matches = "^(?!changeme).*",
        disabledReason = "This test is only meant for manual testing. To run it, set the value of " +
            "semaphoreApiKey in ./gradle-local.properties in this repository to a valid Semaphore API key. " +
            "You will need to set semaphoreApiHost as well. After running, you can use Query Console to " +
            "see the results."
    )
    @Test
    void adHocTest() {
        // Change this as needed.
        String path = "src/test/resources/extraction-files/armstrong_neil.pdf";

        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load(path)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/aaa/output.pdf")
            .option(Options.WRITE_COLLECTIONS, "binary-files")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_CLASSIFIER_APIKEY, System.getenv("SEMAPHORE_API_KEY"))
            .option(Options.WRITE_CLASSIFIER_HOST, System.getenv("SEMAPHORE_HOST"))
            .option(Options.WRITE_CLASSIFIER_PATH, System.getenv("SEMAPHORE_PATH"))
            .mode(SaveMode.Append)
            .save();

        assertInCollections("/aaa/output.pdf", "binary-files");
    }
}
