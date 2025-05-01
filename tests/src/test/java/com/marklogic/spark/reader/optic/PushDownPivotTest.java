/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownPivotTest extends AbstractPushDownTest {

    /**
     * A pivot call results in Spark running two separate jobs. First, Spark will do a groupBy on Date via the pivot()
     * call (and with no aggregate functions), thus getting back all the Date values (and there are 5 unique ones).
     * It will then run a second job with a groupBy on CitationID and Date and an aggregate of MAX on LuckyNumber.
     */
    @Test
    void test() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .pivot("Date")
            .max("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogic(10, "Spark should have read 5 rows in the first job, when it " +
            "retrieved the 5 unique Date values. It should then have read 5 more rows in the second job, when it did " +
            "a groupBy on CitationID + Date.");

        assertEquals(1l, rows.get(0).get(0));
        assertEquals(4, (int) rows.get(0).getAs("2022-07-13"));

        assertEquals(2l, rows.get(1).get(0));
        assertEquals(8, (int) rows.get(1).getAs("2022-05-11"));

        assertEquals(3l, rows.get(2).get(0));
        assertEquals(12, (int) rows.get(2).getAs("2022-04-11"));

        assertEquals(4l, rows.get(3).get(0));
        assertEquals(13, (int) rows.get(3).getAs("2022-06-10"));

        assertEquals(5l, rows.get(4).get(0));
        assertEquals(15, (int) rows.get(4).getAs("2022-07-12"));
    }
}
