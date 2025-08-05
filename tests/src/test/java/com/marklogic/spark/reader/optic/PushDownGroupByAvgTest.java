/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownGroupByAvgTest extends AbstractPushDownTest {

    @Test
    void groupByAvg() {
        verifyRows(
            "avg(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .avg("LuckyNumber")
                .orderBy("CitationID")
        );
    }

    /**
     * This test verifies that when Spark breaks the AVG into a SUM and a COUNT - you can inspect the logging to see
     * this happen - our connector correctly pushes both down and returns the correct columns, thus allowing Spark to
     * calculate the AVG itself.
     */
    @Test
    void multiplePartitions() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            .groupBy("CitationID")
            .avg("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogicBetween(5, 11,
            "Because two partitions are used, the count of rows from MarkLogic is expected to be more than 5 but not " +
                "more than 10, as each request to MarkLogic should return at least one row but not more than 5. " +
                "There is a remote chance that all rows occurred in one partition and this assertion will fail. ");
        verifyRowsHaveCorrectValues(rows, "avg(LuckyNumber)");
    }

    @Test
    void aggAvg() {
        verifyRows(
            "avg(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .agg(avg("LuckyNumber"))
                .orderBy("CitationID")
        );
    }

    @Test
    void qualifiedColumnNames() {
        verifyRows(
            "avg(Medical.Authors.LuckyNumber)",
            newDefaultReader()
                .load()
                .groupBy("`Medical.Authors.CitationID`")
                .avg("`Medical.Authors.LuckyNumber`")
                .orderBy("`Medical.Authors.CitationID`")
        );
    }

    private void verifyRows(String columnName, Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        assertRowsReadFromMarkLogic(5, "Expecting one row read back for each CitationID value");
        verifyRowsHaveCorrectValues(rows, columnName);
    }

    private void verifyRowsHaveCorrectValues(List<Row> rows, String columnName) {
        assertEquals(2.5, (double) rows.get(0).getAs(columnName));
        assertEquals(6.5, (double) rows.get(1).getAs(columnName));
        assertEquals(10.5, (double) rows.get(2).getAs(columnName));
        assertEquals(13.0, (double) rows.get(3).getAs(columnName));
        assertEquals(14.5, (double) rows.get(4).getAs(columnName));
    }
}
