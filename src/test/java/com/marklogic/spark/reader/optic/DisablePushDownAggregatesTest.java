/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DisablePushDownAggregatesTest extends AbstractPushDownTest {

    @Test
    void disabled() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_PUSH_DOWN_AGGREGATES, false)
            .load()
            .groupBy("CitationID")
            .avg("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogic(15, "Because push down of aggregates is disabled, all 15 author " +
            "rows should have been read from MarkLogic.");

        // Averages should still be calculated correctly by Spark.
        String columnName = "avg(LuckyNumber)";
        assertEquals(2.5, (double) rows.get(0).getAs(columnName));
        assertEquals(6.5, (double) rows.get(1).getAs(columnName));
        assertEquals(10.5, (double) rows.get(2).getAs(columnName));
        assertEquals(13.0, (double) rows.get(3).getAs(columnName));
        assertEquals(14.5, (double) rows.get(4).getAs(columnName));
    }
}
