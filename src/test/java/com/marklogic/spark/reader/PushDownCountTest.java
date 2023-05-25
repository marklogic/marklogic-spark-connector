package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushDownCountTest extends AbstractPushDownTest {

    @Test
    void count() {
        long count = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, 2)
            .option(Options.READ_BATCH_SIZE, 1000)
            .load()
            .count();

        assertEquals(15, count, "Expecting all 15 authors to be counted");
        assertEquals(1, countOfRowsReadFromMarkLogic, "When count() is used, only one call should be made to " +
            "MarkLogic, regardless of the number of partitions and the batch size. The connector is expected to both " +
            "modify the inferred schema so that a schema with just one column - 'Count' - is used. And it is also " +
            "expected to modify the plan analysis so that a single bucket is used. That is based on the assumption " +
            "that regardless of the number of matching rows, MarkLogic can efficiently determine a count in a single " +
            "request.");
    }

    @Test
    void groupByAndCount() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, QUERY_WITH_NO_QUALIFIER)
            .load()
            .groupBy("CitationID")
            .count()
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(15, countOfRowsReadFromMarkLogic, "groupBy + count is not yet being pushed down to MarkLogic; " +
            "only count() by itself is being pushed down. So expecting all rows to be read for now.");

        assertEquals(4, (long) rows.get(0).getAs("count"));
        assertEquals(4, (long) rows.get(1).getAs("count"));
        assertEquals(4, (long) rows.get(2).getAs("count"));
        assertEquals(1, (long) rows.get(3).getAs("count"));
        assertEquals(2, (long) rows.get(4).getAs("count"));
    }
}
