package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.min;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PushDownGroupByMinTest extends AbstractPushDownTest {

    @Test
    void groupByMin() {
        verifyRows(
            "min(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .min("LuckyNumber")
                .orderBy("CitationID")
        );
    }

    @Test
    void aggMin() {
        verifyRows(
            "min(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .agg(min("LuckyNumber"))
                .orderBy("CitationID")
        );
    }

    @Test
    void qualifiedColumnNames() {
        verifyRows(
            "min(Medical.Authors.LuckyNumber)",
            newDefaultReader()
                .load()
                .groupBy("`Medical.Authors.CitationID`")
                .min("`Medical.Authors.LuckyNumber`")
                .orderBy("`Medical.Authors.CitationID`")
        );
    }

    private void verifyRows(String columnName, Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        assertRowsReadFromMarkLogic(5, "Expecting one row read back for each CitationID value");

        assertEquals(1, (int) rows.get(0).getAs(columnName));
        assertEquals(5, (int) rows.get(1).getAs(columnName));
        assertEquals(9, (int) rows.get(2).getAs(columnName));
        assertEquals(13, (int) rows.get(3).getAs(columnName));
        assertEquals(14, (int) rows.get(4).getAs(columnName));
    }
}
