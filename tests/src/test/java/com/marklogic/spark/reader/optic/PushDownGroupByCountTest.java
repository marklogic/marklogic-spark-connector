/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownGroupByCountTest extends AbstractPushDownTest {

    @Test
    void groupByWithNoQualifier() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .count()
            .orderBy("CitationID")
            .collectAsList();

        verifyGroupByWasPushedDown(rows);
        assertEquals(1l, (long) rows.get(0).getAs("CitationID"));
    }

    @Test
    void groupByMultipleColumns() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID", "Date")
            .count()
            .orderBy("CitationID")
            .collectAsList();

        verifyGroupByWasPushedDown(rows);

        assertEquals(1l, (long) rows.get(0).getAs("CitationID"));
        assertEquals("2022-07-13", rows.get(0).getAs("Date").toString());
        assertEquals(2l, (long) rows.get(1).getAs("CitationID"));
        assertEquals("2022-05-11", rows.get(1).getAs("Date").toString());
    }

    @Test
    void noRowsFound() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .groupBy("CitationID")
            .count()
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(0, rows.size());
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void groupByWithView() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', 'example')")
            .load()
            .groupBy("`example.CitationID`")
            .count()
            .orderBy("`example.CitationID`")
            .collectAsList();

        verifyGroupByWasPushedDown(rows);
        assertEquals(1l, (long) rows.get(0).getAs("example.CitationID"));
    }

    @Test
    void groupByWithSchemaAndView() {
        List<Row> rows = newDefaultReader()
            .load()
            .groupBy("`Medical.Authors.CitationID`")
            .count()
            .orderBy("`Medical.Authors.CitationID`")
            .collectAsList();

        verifyGroupByWasPushedDown(rows);
        assertEquals(1l, (long) rows.get(0).getAs("Medical.Authors.CitationID"));
    }

    @Test
    void groupByMultipleColumnsAndSchemaAndView() {
        List<Row> rows = newDefaultReader()
            .load()
            .groupBy("`Medical.Authors.CitationID`", "`Medical.Authors.Date`")
            .count()
            .orderBy("`Medical.Authors.CitationID`")
            .collectAsList();

        verifyGroupByWasPushedDown(rows);

        verifyGroupByWasPushedDown(rows);
        assertEquals(1l, (long) rows.get(0).getAs("Medical.Authors.CitationID"));
        assertEquals("2022-07-13", rows.get(0).getAs("Medical.Authors.Date").toString());
    }


    @Test
    void groupByCountLimitOrderBy() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .count()
            .limit(4)
            // When the user puts the orderBy after limit, Spark doesn't push the orderBy down. Spark will instead
            // apply the orderBy itself.
            .orderBy("count")
            .collectAsList();

        assertEquals(4, rows.size());
        assertRowsReadFromMarkLogic(4);
        assertEquals(4l, (long) rows.get(0).getAs("CitationID"));
        assertEquals(1l, (long) rows.get(0).getAs("count"));
    }

    @Test
    void groupByCountOrderByLimit() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .count()
            // If the user puts orderBy before limit, Spark will send "COUNT(*)" as the column name for the orderBy.
            // The connector is expected to translate that into "count"; not sure how it should work otherwise. Spark
            // is expected to push down the limit as well.
            .orderBy("count")
            .limit(4)
            .collectAsList();

        assertEquals(4, rows.size());
        if (isSpark340OrHigher()) {
            assertRowsReadFromMarkLogic(4);
        } else {
            assertRowsReadFromMarkLogic(5, "With Spark 3.3.x, the limit is not pushed down, perhaps " +
                "when groupBy is called as well. Spark 3.4.0 fixes this so that the limit is pushed down. So for 3.3.x, " +
                "we expect all 5 rows - one per CitationID.");
        }
        assertEquals(4l, (long) rows.get(0).getAs("CitationID"));
        assertEquals(1l, (long) rows.get(0).getAs("count"));
    }

    private void verifyGroupByWasPushedDown(List<Row> rows) {
        /**
         * Note that in Spark 3.3.0, there seems to be a bug where groupBy+count are not always pushed down. That's not
         * an issue in Spark 3.3.2, so the behavior in 3.3.0 seems to be considered buggy and thus fixed in 3.3.2.
         * While AWS Glue and EMR are both currently using 3.3.0 and not 3.3.2, we'd rather test against the latest
         * bugfix release to ensure we're in sync with that and not writing test assertions against what's considered
         * buggy behavior in 3.3.0.
         */
        assertRowsReadFromMarkLogic(5, "groupBy should be pushed down to MarkLogic when used with " +
            "count, and since there are 5 CitationID values, 5 rows should be returned.");

        assertEquals(4, (long) rows.get(0).getAs("count"));
        assertEquals(4, (long) rows.get(1).getAs("count"));
        assertEquals(4, (long) rows.get(2).getAs("count"));
        assertEquals(1, (long) rows.get(3).getAs("count"));
        assertEquals(2, (long) rows.get(4).getAs("count"));
    }
}
