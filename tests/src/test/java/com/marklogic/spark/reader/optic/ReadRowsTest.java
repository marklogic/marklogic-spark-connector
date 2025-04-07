/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */

package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsTest extends AbstractIntegrationTest {

    @Test
    void validPartitionCountAndBatchSize() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, 3)
            .option(Options.READ_BATCH_SIZE, 5)

            // Including this only to ensure it doesn't cause errors.
            .option(Options.READ_LOG_PROGRESS, 5)

            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Expecting all 15 rows to be returned from the view");

        rows.forEach(row -> {
            long id = row.getLong(0);
            assertEquals(id, (long) row.getAs("Medical.Authors.CitationID"), "Verifying that the first column has " +
                "the citation ID and can be accessed via a fully qualified column name.");

            assertTrue(id >= 1 && id <= 5, "The citation ID is expected to be the first column value, and based on our " +
                "test data, it should have a value from 1 to 5; actual value: " + id);

            assertEquals(9, row.size(), "Expecting the row to have 9 columns since the TDE defines 9 columns, and the " +
                "Spark schema is expected to be inferred from the TDE.");
        });
    }

    @Test
    void emptyQualifier() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', '')")
            .load()
            .collectAsList();

        assertEquals(15, rows.size());

        rows.forEach(row -> {
            long id = row.getLong(0);
            assertEquals(id, (long) row.getAs("CitationID"), "Verifying that the first column has " +
                "the citation ID and can be accessed via a column name with no qualifier.");
        });
    }

    @Test
    void queryReturnsZeroRows() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .collectAsList();

        assertEquals(0, rows.size(), "When no rows exist, the /v1/rows endpoint currently (as of 11.0) throws a " +
            "fairly cryptic error. A user does not expect an error in this scenario, so the connector is expected " +
            "to catch that error and return a valid dataset with zero rows in it.");
    }

    @Test
    void invalidQuery() {
        DataFrameReader reader = newDefaultReader().option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'ViewNotFound')");
        RuntimeException ex = assertThrows(RuntimeException.class, reader::load);

        assertTrue(ex.getMessage().startsWith("Unable to run Optic query op.fromView('Medical', 'ViewNotFound'); cause: "),
            "If the query throws an error for any reason other than no rows being found, the error should be wrapped " +
                "in a new error with a message containing the user's query to assist with debugging; actual " +
                "message: " + ex.getMessage());
    }

    @Test
    void invalidCredentials() {
        DataFrameReader reader = newDefaultReader().option(Options.CLIENT_PASSWORD, "invalid password");
        RuntimeException ex = assertThrows(RuntimeException.class, reader::load);

        assertEquals("Unable to connect to MarkLogic; status code: 401; error message: Unauthorized", ex.getMessage(),
            "The connector should verify that it can connect to MarkLogic before it tries to analyze the user's query. " +
                "This allows for a more helpful error message so that the user can focus on fixing their connection and " +
                "authentication properties.");
    }

    @Test
    void nonNumericPartitionCount() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, "abc")
            .load();
        ConnectorException ex = assertThrows(ConnectorException.class, reader::count);
        assertEquals("The value of 'spark.marklogic.read.numPartitions' must be numeric.", ex.getMessage());
    }

    @Test
    void partitionCountLessThanOne() {
        Dataset<Row> reader = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, "0")
            .load();

        ConnectorException ex = assertThrows(ConnectorException.class, reader::count);
        assertEquals("The value of 'spark.marklogic.read.numPartitions' must be 1 or greater.", ex.getMessage());
    }

    @Test
    void nonNumericBatchSize() {
        DataFrameReader reader = newDefaultReader().option(Options.READ_BATCH_SIZE, "abc");
        ConnectorException ex = assertThrows(ConnectorException.class, reader::load);
        assertEquals("The value of 'spark.marklogic.read.batchSize' must be numeric.", ex.getMessage());
    }

    @Test
    void batchSizeLessThanZero() {
        DataFrameReader reader = newDefaultReader().option(Options.READ_BATCH_SIZE, "-1");
        ConnectorException ex = assertThrows(ConnectorException.class, reader::load);
        assertEquals("The value of 'spark.marklogic.read.batchSize' must be 0 or greater.", ex.getMessage());
    }
}
