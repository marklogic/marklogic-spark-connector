/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractPushDownTest extends AbstractIntegrationTest {

    static final String QUERY_WITH_NO_QUALIFIER = "op.fromView('Medical', 'Authors', '')";
    static final String QUERY_ORDERED_BY_CITATION_ID = "op.fromView('Medical', 'Authors', '').orderBy(op.col('CitationID'))";

    private long countOfRowsReadFromMarkLogic;

    @BeforeEach
    void setup() {
        // This is used to track how many rows were read from MarkLogic. It's used to ensure that the filtering is
        // correctly pushed down to MarkLogic as opposed to being handled by Spark, which should make operations
        // faster as MarkLogic is returning fewer rows. A synchronized method is used in case the test uses multiple
        // partitions, as each will run on a separate thread.
        OpticPartitionReader.totalRowCountListener = this::addToRowCount;
    }

    @Override
    protected DataFrameReader newDefaultReader(SparkSession session) {
        return super.newDefaultReader(session)
            // Default to a single call to MarkLogic for push down tests to ensure that assertions on row counts are
            // accurate (and via DEVEXP-488, the batch size is expected to be set to zero when an aggregate is pushed
            // down). Any tests that care about having more than one partition are expected to override this.
            .option(Options.READ_NUM_PARTITIONS, 1);
    }

    protected final boolean isSparkThreeFive() {
        // The pushdown support appears to have changed between Spark 3.4 and 3.5. In a scenario with a single partition
        // reader, logging show the reader being created twice and performing its query twice, resulting in an unexpected
        // number of rows being read from MarkLogic. The correct number of rows are present in the Spark dataframe,
        // but assertions on how many rows were read from MarkLogic fail. Will investigate further when we start
        // building against Spark 3.5 or higher.
        return sparkSession.version().startsWith("3.5");
    }

    protected final void assertRowsReadFromMarkLogic(long expectedCount) {
        if (!isSparkThreeFive()) {
            assertEquals(expectedCount, countOfRowsReadFromMarkLogic);
        }
    }

    protected final void assertRowsReadFromMarkLogic(long expectedCount, String message) {
        if (!isSparkThreeFive()) {
            assertEquals(expectedCount, countOfRowsReadFromMarkLogic, message);
        }
    }

    protected final void assertRowsReadFromMarkLogicGreaterThan(long expectedCount, String message) {
        if (!isSparkThreeFive()) {
            assertTrue(countOfRowsReadFromMarkLogic > expectedCount,
                message + "; actual count: " + countOfRowsReadFromMarkLogic);
        }
    }

    protected final void assertRowsReadFromMarkLogicBetween(long min, long max, String message) {
        if (!isSparkThreeFive()) {
            assertTrue(countOfRowsReadFromMarkLogic > min && countOfRowsReadFromMarkLogic < max,
                message + "; actual count: " + countOfRowsReadFromMarkLogic);
        }
    }

    private synchronized void addToRowCount(long totalRowCount) {
        countOfRowsReadFromMarkLogic += totalRowCount;
    }

    protected Dataset<Row> newDatasetOrderedByCitationIDWithOneBucket() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_ORDERED_BY_CITATION_ID)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();
    }

}
