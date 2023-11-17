package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.mean;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * In Spark, avg == mean, so no new code needed, just ensuring that "mean" behaves the same as "avg".
 */
public class PushDownGroupByMeanTest extends AbstractPushDownTest {

    @Test
    void groupByMean() {
        verifyRows(
            "avg(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .mean("LuckyNumber")
                .orderBy("CitationID")
        );
    }

    @Test
    void aggMean() {
        verifyRows(
            "avg(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .agg(mean("LuckyNumber"))
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
                .mean("`Medical.Authors.LuckyNumber`")
                .orderBy("`Medical.Authors.CitationID`")
        );
    }

    private void verifyRows(String columnName, Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        assertRowsReadFromMarkLogic(5, "Expecting one row read back for each CitationID value");

        assertEquals(2.5, (double) rows.get(0).getAs(columnName));
        assertEquals(6.5, (double) rows.get(1).getAs(columnName));
        assertEquals(10.5, (double) rows.get(2).getAs(columnName));
        assertEquals(13.0, (double) rows.get(3).getAs(columnName));
        assertEquals(14.5, (double) rows.get(4).getAs(columnName));
    }
}
