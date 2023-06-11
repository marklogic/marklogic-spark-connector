package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushDownGroupByManyAggregatesTest extends AbstractPushDownTest {

    @Test
    void test() {
        List<Row> rows =
            newDefaultReader()
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
        assertEquals(5, countOfRowsReadFromMarkLogic);

        String column = "sum(LuckyNumber)";
        assertEquals(10, (long) rows.get(0).getAs(column));
        assertEquals(26, (long) rows.get(1).getAs(column));
        assertEquals(42, (long) rows.get(2).getAs(column));
        assertEquals(13, (long) rows.get(3).getAs(column));
        assertEquals(29, (long) rows.get(4).getAs(column));

        column = "avg(LuckyNumber)";
        assertEquals(2.5, (double) rows.get(0).getAs(column));
        assertEquals(6.5, (double) rows.get(1).getAs(column));
        assertEquals(10.5, (double) rows.get(2).getAs(column));
        assertEquals(13.0, (double) rows.get(3).getAs(column));
        assertEquals(14.5, (double) rows.get(4).getAs(column));

        column = "min(LuckyNumber)";
        assertEquals(1, (int) rows.get(0).getAs(column));
        assertEquals(5, (int) rows.get(1).getAs(column));
        assertEquals(9, (int) rows.get(2).getAs(column));
        assertEquals(13, (int) rows.get(3).getAs(column));
        assertEquals(14, (int) rows.get(4).getAs(column));

        column = "max(LuckyNumber)";
        assertEquals(4, (int) rows.get(0).getAs(column));
        assertEquals(8, (int) rows.get(1).getAs(column));
        assertEquals(12, (int) rows.get(2).getAs(column));
        assertEquals(13, (int) rows.get(3).getAs(column));
        assertEquals(15, (int) rows.get(4).getAs(column));

        column = "count(LuckyNumber)";
        assertEquals(4, (long) rows.get(0).getAs(column));
        assertEquals(4, (long) rows.get(1).getAs(column));
        assertEquals(4, (long) rows.get(2).getAs(column));
        assertEquals(1, (long) rows.get(3).getAs(column));
        assertEquals(2, (long) rows.get(4).getAs(column));
    }
}
