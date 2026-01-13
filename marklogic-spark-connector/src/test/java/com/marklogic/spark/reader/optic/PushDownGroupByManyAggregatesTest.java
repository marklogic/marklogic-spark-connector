/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownGroupByManyAggregatesTest extends AbstractPushDownTest {

    private List<Row> rows;

    @Test
    void test() {
        rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .agg(
                sum("LuckyNumber"),
                avg("LuckyNumber"),
                min("LuckyNumber"),
                max("LuckyNumber"),
                // Spark still maps this to a CountStar, even though a column name is specified.
                count("LuckyNumber")
            )
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogic(5);

        verifySumColumn();
        verifyAvgColumn();
        verifyMinColumn();
        verifyMaxColumn();
        verifyCountColumn();
    }

    private void verifySumColumn() {
        String column = "sum(LuckyNumber)";
        assertEquals(10, (long) rows.get(0).getAs(column));
        assertEquals(26, (long) rows.get(1).getAs(column));
        assertEquals(42, (long) rows.get(2).getAs(column));
        assertEquals(13, (long) rows.get(3).getAs(column));
        assertEquals(29, (long) rows.get(4).getAs(column));
    }

    private void verifyAvgColumn() {
        String column = "avg(LuckyNumber)";
        assertEquals(2.5, (double) rows.get(0).getAs(column));
        assertEquals(6.5, (double) rows.get(1).getAs(column));
        assertEquals(10.5, (double) rows.get(2).getAs(column));
        assertEquals(13.0, (double) rows.get(3).getAs(column));
        assertEquals(14.5, (double) rows.get(4).getAs(column));
    }

    private void verifyMinColumn() {
        String column = "min(LuckyNumber)";
        assertEquals(1, (int) rows.get(0).getAs(column));
        assertEquals(5, (int) rows.get(1).getAs(column));
        assertEquals(9, (int) rows.get(2).getAs(column));
        assertEquals(13, (int) rows.get(3).getAs(column));
        assertEquals(14, (int) rows.get(4).getAs(column));
    }

    private void verifyMaxColumn() {
        String column = "max(LuckyNumber)";
        assertEquals(4, (int) rows.get(0).getAs(column));
        assertEquals(8, (int) rows.get(1).getAs(column));
        assertEquals(12, (int) rows.get(2).getAs(column));
        assertEquals(13, (int) rows.get(3).getAs(column));
        assertEquals(15, (int) rows.get(4).getAs(column));
    }

    private void verifyCountColumn() {
        String column = "count(LuckyNumber)";
        assertEquals(4, (long) rows.get(0).getAs(column));
        assertEquals(4, (long) rows.get(1).getAs(column));
        assertEquals(4, (long) rows.get(2).getAs(column));
        assertEquals(1, (long) rows.get(3).getAs(column));
        assertEquals(2, (long) rows.get(4).getAs(column));
    }


}
