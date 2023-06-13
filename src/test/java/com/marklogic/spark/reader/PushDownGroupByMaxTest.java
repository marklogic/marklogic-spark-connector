package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.max;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PushDownGroupByMaxTest extends AbstractPushDownTest {

    @Test
    void groupByMax() {
        verifyRows(
            "max(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .max("LuckyNumber")
                .orderBy("CitationID")
        );
    }

    @Test
    void multiplePartitions() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load()
            .groupBy("CitationID")
            .max("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertTrue(countOfRowsReadFromMarkLogic > 5, "Because 2 partitions exist, it is expected that more than " +
            "5 rows in total are ready by the two partition readers, and then Spark will apply the aggregation on the " +
            "two sets of rows returned by the partition readers. There's a slight chance this assertion will fail in " +
            "the event that all 15 rows are in one partition. Actual count: " + countOfRowsReadFromMarkLogic);
        verifyRowsHaveCorrectValues(rows, "max(LuckyNumber)");
    }

    @Test
    void customBatchSize() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 5)
            .load()
            .groupBy("CitationID")
            .max("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertTrue(countOfRowsReadFromMarkLogic > 5, "If the user specifies a batch size, then the connector should " +
            "not default it to zero when an aggregate is pushed down; the assumption is that the user has a good " +
            "reason for specifying a batch size. With 1 partition and 15 matching rows and a batch size of 5, 3 " +
            "requests should be made to MarkLogic, and it's expected that more than 5 rows are returned across " +
            "those 3 requests (unless each of the 5 CitationID values have their rows in the same partition, which " +
            "is very unlikely). Actual count: " + countOfRowsReadFromMarkLogic);
        verifyRowsHaveCorrectValues(rows, "max(LuckyNumber)");
    }

    @Test
    void aggMax() {
        verifyRows(
            "max(LuckyNumber)",
            newDefaultReader()
                .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
                .load()
                .groupBy("CitationID")
                .agg(max("LuckyNumber"))
                .orderBy("CitationID")
        );
    }

    @Test
    void qualifiedColumnNames() {
        verifyRows(
            "max(Medical.Authors.LuckyNumber)",
            newDefaultReader()
                .load()
                .groupBy("`Medical.Authors.CitationID`")
                .max("`Medical.Authors.LuckyNumber`")
                .orderBy("`Medical.Authors.CitationID`")
        );
    }

    private void verifyRows(String columnName, Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        assertEquals(5, rows.size());
        assertEquals(5, countOfRowsReadFromMarkLogic, "Expecting one row read back for each CitationID value");
        verifyRowsHaveCorrectValues(rows, columnName);
    }

    private void verifyRowsHaveCorrectValues(List<Row> rows, String columnName) {
        assertEquals(4, (int) rows.get(0).getAs(columnName));
        assertEquals(8, (int) rows.get(1).getAs(columnName));
        assertEquals(12, (int) rows.get(2).getAs(columnName));
        assertEquals(13, (int) rows.get(3).getAs(columnName));
        assertEquals(15, (int) rows.get(4).getAs(columnName));
    }
}
