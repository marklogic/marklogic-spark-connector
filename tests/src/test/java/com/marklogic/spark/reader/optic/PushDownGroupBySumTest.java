/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.sum;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownGroupBySumTest extends AbstractPushDownTest {

    @Test
    void groupBySum() {
        verifyRows(
            "sum(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .sum("LuckyNumber")
                .orderBy("CitationID")
        );
    }

    @Test
    void aggSum() {
        verifyRows(
            "sum(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .agg(sum("LuckyNumber"))
                .orderBy("CitationID")
        );
    }

    @Test
    void qualifiedColumnNames() {
        verifyRows(
            "sum(Medical.Authors.LuckyNumber)",
            newDefaultReader()
                .load()
                .groupBy("`Medical.Authors.CitationID`")
                .sum("`Medical.Authors.LuckyNumber`")
                .orderBy("`Medical.Authors.CitationID`")
        );
    }

    private void verifyRows(String columnName, Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        assertRowsReadFromMarkLogic(5, "Expecting one row read back for each CitationID value");

        assertEquals(10, (long) rows.get(0).getAs(columnName));
        assertEquals(26, (long) rows.get(1).getAs(columnName));
        assertEquals(42, (long) rows.get(2).getAs(columnName));
        assertEquals(13, (long) rows.get(3).getAs(columnName));
        assertEquals(29, (long) rows.get(4).getAs(columnName));
    }
}
