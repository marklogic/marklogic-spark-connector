/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class WritePartialBatchTest extends AbstractWriteTest {

    @Test
    void threeOutOfFourShouldFail() {
        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files")
            .repartition(1) // Forces all 4 docs to be written in a single batch.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
        );

        assertCollectionSize("Only the JSON document should have succeeded; error messages should have been logged " +
            "for the other 3 documents.", "partial-batch", 1);
    }

    @Test
    void shouldThrowError() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_URI_SUFFIX, ".json")
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("Document is not JSON"), "Verifying that trying to write non-JSON " +
            "documents with a .json extension should produce an error; unexpected error: " + ex.getMessage());
    }

    @Test
    void fiveOutOfTenShouldFailWithTwoBatches() {
        defaultWrite(newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/some-bad-json-docs.zip")
            .repartition(1) // Forces a single partition writer.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "partial-batch")
            .option(Options.WRITE_URI_REPLACE, ".*/spark-json,''")
            .option(Options.WRITE_ABORT_ON_FAILURE, false)
            .option(Options.WRITE_BATCH_SIZE, 5) // Forces two batches, each of which should have 1 or more failures.
        );

        assertCollectionSize(
            "5 of the docs in the zip file are valid JSON docs, so those should all be written, regardless of how " +
                "many partition writers and batches are created. Errors should be logged for the 5 invalid JSON docs.",
            "partial-batch", 5
        );
    }
}
