/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GroupByDuplicateColumnNamesTest extends AbstractPushDownTest {

    @Test
    void sameColumnNameTwice() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '')")
            .load()
            .groupBy("CitationID", "CitationID")
            .sum("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size(), "This verifies that the duplicate groupBy column name is not passed to Optic, " +
            "which would cause an error of 'Grouping key shouldn't have duplicates'.");
        assertRowsReadFromMarkLogic(5, "The groupBy/sum should have been pushed down.");

        String message = "The first two columns are expected to both be named " +
            "'CitationID', which is a little surprising but appears to be expected by Spark. And the " +
            "columns should have the same CitationID value.";
        for (int i = 0; i < 5; i++) {
            Row row = rows.get(i);
            int expectedCitationID = i + 1;
            assertEquals(expectedCitationID, row.getLong(0), message);
            assertEquals(expectedCitationID, row.getLong(1), message);
        }
    }

    @Test
    void sameColumnViaFieldReference() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '')")
            .load()
            .withColumn("otherID", new Column("CitationID"))
            .groupBy("otherID", "CitationID")
            .sum("LuckyNumber")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogic(5, "The groupBy/sum should have been pushed down.");

        String message = "withColumn does not seem to work correctly for our connector when it only creates an alias. Our " +
            "connector is not passed 'CitationID' and 'otherID', but rather it gets 'CitationID' twice. So our " +
            "connector doesn't even know about 'otherID'. The connector then returns rows with the correct values for " +
            "'CitationID' and 'SUM(LuckyNumber)', but for an unknown reason, JsonRowDeserializer does not copy the " +
            "'CitationID' value into the 'CitationID' column. And it understandably leaves the 'otherID' column " +
            "blank since it doesn't know about that column.";
        rows.forEach(row -> {
            assertEquals(0, row.getLong(0), message);
            assertEquals(0, row.getLong(1), message);
        });
    }

    @Test
    void groupByTwoColumns() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '')")
            .load()
            .withColumn("CitationIDPlusOne", new Column("CitationID").plus(1))
            .groupBy("CitationID", "CitationIDPlusOne")
            .sum("LuckyNumber")
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());
        assertRowsReadFromMarkLogic(15, "Surprisingly, Spark never calls pushDownAggregation in this scenario. The " +
            "use of withColumn with a new set of values seems to prevent that, but it is not known why. We get back " +
            "the correct data from Spark, but the groupBy/sum are not pushed down. ");
        for (int i = 0; i < 5; i++) {
            Row row = rows.get(i);
            assertEquals(i + 1, row.getLong(0));
            assertEquals(i + 2, row.getLong(1));
        }
    }
}
