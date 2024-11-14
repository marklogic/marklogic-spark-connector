/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
import com.marklogic.spark.RequiresMarkLogic11OrLower;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * As of 2024-10-29, this is mysteriously failing with 8016 connection issues on Jenkins. Does not fail on MarkLogic
 * 11 though. Will investigate more soon.
 */
@ExtendWith(RequiresMarkLogic11OrLower.class)
class PushDownFilterTest extends AbstractPushDownTest {

    /**
     * equalTo has several tests to verify that filter/where work the same (or at least appear to) and they can be
     * combined as well. No need to re-test that for every other filter type.
     */
    @Test
    void equalToWithFilter() {
        assertEquals(4, getCountOfRowsWithFilter("CitationID == 1"));
        assertRowsReadFromMarkLogic(4);
    }

    @Test
    void equalToWithSchemaAndViewQualifier() {
        assertEquals(4, newDefaultReader()
            .load()
            .filter("`Medical.Authors.CitationID` == 1")
            .collectAsList()
            .size(), "Verifying that a filter with a fully-qualified column name still works correctly.");
        assertRowsReadFromMarkLogic(4);
    }

    @Test
    void equalToWithViewQualifier() {
        assertEquals(4, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', 'myView')")
            .load()
            .filter("`myView.CitationID` == 1")
            .collectAsList()
            .size(), "Verifying that a filter with a view-qualified column name still works correctly.");
        assertRowsReadFromMarkLogic(4);
    }

    @Test
    void noRowsFound() {
        assertEquals(0, newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .filter("CitationID == 1")
            .collectAsList()
            .size());
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void equalToWithWhere() {
        assertEquals(2, getCountOfRowsWithFilter("CitationID = 5"));
        assertRowsReadFromMarkLogic(2);
    }

    @Test
    void equalToWithString() {
        assertEquals(0, getCountOfRowsWithFilter("LastName == 'No match'"));
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void equalToWithWhereAndFilter() {
        assertEquals(1, newDataset().where("CitationID = 1").filter("LastName == 'Golby'").count());
        assertRowsReadFromMarkLogic(1);
    }

    @Test
    void equalNullSafe() {
        assertEquals(1, newDataset().filter(new Column("BooleanValue").eqNullSafe(true)).count(),
            "The intent of eqNullSafe appears to be that errors won't occur when a row has a column value of 'null' " +
                "for the filtered column. 13 of the 15 author rows have a value of 'null'. This does not appear to be " +
                "an issue for Optic, so eqNullSafe appears to be equivalent to equalTo.");
    }

    @Test
    void greaterThan() {
        assertEquals(3, getCountOfRowsWithFilter("CitationID > 3"));
        assertRowsReadFromMarkLogic(3);
    }

    @Test
    void greaterThanOrEqual() {
        assertEquals(7, getCountOfRowsWithFilter("CitationID >= 3"));
        assertRowsReadFromMarkLogic(7);
    }

    @Test
    void lessThan() {
        assertEquals(4, getCountOfRowsWithFilter("CitationID < 2"));
        assertRowsReadFromMarkLogic(4);
    }

    @Test
    void lessThanOrEqual() {
        assertEquals(8, getCountOfRowsWithFilter("CitationID <= 2"));
        assertRowsReadFromMarkLogic(8);
    }

    /**
     * This doesn't result in an "And" filter being created; Spark just passes in two EqualTo filters that are
     * naturally AND'ed together. The orWithAnd test requires that an "And" filter be supported.
     */
    @Test
    void and() {
        assertEquals(9, getCountOfRowsWithFilter("CitationID < 5 AND CitationID > 1"));
        assertRowsReadFromMarkLogic(9);
    }

    /**
     * Captured in MLE-13771.
     */
    @Test
    void multipleFilters() {
        Dataset<Row> dataset = newDataset();
        dataset = dataset
            .filter(dataset.col("LastName").contains("umbe"))
            .filter(dataset.col("CitationID").equalTo(5));

        List<Row> rows = dataset.collectAsList();
        assertEquals(1, rows.size());
        assertRowsReadFromMarkLogic(1, "The two filters should be tossed into separate Optic 'where' clauses so " +
            "so that an op.sqlCondition is not improperly added to an op.and, which Optic does not allow. The " +
            "filters should thus both be pushed down successfully");
    }

    @Test
    void orClauseWithSqlCondition() {
        assertEquals(2, getCountOfRowsWithFilter("LastName LIKE '%ool%' OR LastName LIKE '%olb%'"));
        assertRowsReadFromMarkLogic(15, "An OR with a sqlCondition cannot be pushed down.");
    }

    @Test
    void notClauseWithSqlCondition() {
        assertEquals(14, getCountOfRowsWithFilter("NOT LastName LIKE '%ool%'"));
        assertRowsReadFromMarkLogic(15, "A NOT with a sqlCondition cannot be pushed down.");
    }

    @Test
    void andClauseWithSqlCondition() {
        assertEquals(1, getCountOfRowsWithFilter("LastName LIKE '%ool%' AND ForeName LIKE '%ivi%'"));
        assertRowsReadFromMarkLogic(1, "Since Spark defaults to AND'ing clauses together, it will not construct " +
            "an 'AND' operator. Instead, it will just sent the two 'LIKE' expressions as two separate filters to " +
            "our connector, and our connector will create two separate Optic sqlCondition's, thus pushing both " +
            "filters down to MarkLogic.");
    }

    @Test
    void or() {
        assertEquals(8, getCountOfRowsWithFilter("CitationID == 1 OR CitationID == 2"));
        assertRowsReadFromMarkLogic(8);
    }

    @Test
    void andWithinOr() {
        // This actually results in an "and" filter being created.
        assertEquals(5, getCountOfRowsWithFilter("(CitationID < 3 AND CitationID > 1) OR CitationID == 4"));
        assertRowsReadFromMarkLogic(5,
            "Expecting 4 authors with a CitationID of 2 and 1 with a CitationID of 4.");
    }

    @Test
    void not() {
        assertEquals(11, getCountOfRowsWithFilter("CitationID != 1"));
        assertRowsReadFromMarkLogic(11);
    }

    @Test
    void multipleLevelsOfBooleanExpressions() {
        assertEquals(3, getCountOfRowsWithFilter("((CitationID == 4 OR CitationID == 5) AND CitationID < 10) OR (CitationID != 3 AND CitationID > 2)"),
            "Expecting the 3 authors with ID of 4 or 5; the query is just intended to be a complicated " +
                "way of asking for those 3 authors, verifying that boolean expressions can be at varying depths.");
    }

    @Test
    void in() {
        assertEquals(7, getCountOfRowsWithFilter("CitationID IN (3,4,5)"));
        assertRowsReadFromMarkLogic(7);
    }

    @Test
    void inWithNoMatches() {
        assertEquals(0, getCountOfRowsWithFilter("LastName in ('Doesnt', 'Match', 'Anything')"));
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void isNotNull() {
        assertEquals(2, newDataset().filter(new Column("BooleanValue").isNotNull()).collectAsList().size());
        assertRowsReadFromMarkLogic(2);
    }

    @Test
    void isNotNullQualified() {
        assertEquals(2, newDefaultReader()
            .load()
            .filter(new Column("`Medical.Authors.BooleanValue`").isNotNull())
            .collectAsList()
            .size());

        assertRowsReadFromMarkLogic(2,
            "2 of the authors are expected to have a BooleanValue column.");
    }

    @Test
    void isNull() {
        assertEquals(13, newDataset()
            .filter(new Column("BooleanValue").isNull())
            .collectAsList()
            .size());
        assertRowsReadFromMarkLogic(13,
            "13 of the authors are expected to have a null BooleanValue column.");
    }

    @Test
    void isNullQualified() {
        assertEquals(13, newDefaultReader()
            .load()
            .filter(new Column("`Medical.Authors.BooleanValue`").isNull())
            .collectAsList().size());
        assertRowsReadFromMarkLogic(13);
    }

    @Test
    void stringContains() {
        List<Row> rows = newDataset().filter(new Column("LastName").contains("umbe")).collectAsList();
        assertEquals(1, rows.size());
        assertRowsReadFromMarkLogic(1);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringContainsNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").contains("umee")).collectAsList().size());
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void stringStartsWith() {
        List<Row> rows = newDataset().filter(new Column("LastName").startsWith("Humb")).collectAsList();
        assertEquals(1, rows.size());
        assertRowsReadFromMarkLogic(1);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringStartsWithNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").startsWith("umbe")).collectAsList().size());
        assertRowsReadFromMarkLogic(0);
    }

    @Test
    void stringEndsWith() {
        List<Row> rows = newDataset().filter(new Column("LastName").endsWith("bee")).collectAsList();
        assertEquals(1, rows.size());
        assertRowsReadFromMarkLogic(1);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringEndsWithNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").endsWith("umbe")).collectAsList().size());
        assertRowsReadFromMarkLogic(0);
    }

    private Dataset<Row> newDataset() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, QUERY_WITH_NO_QUALIFIER)
            .option(Options.READ_PUSH_DOWN_AGGREGATES, false)
            .load();
    }

    private long getCountOfRowsWithFilter(String filter) {
        // collectAsList is used here so we can count how many rows are returned, as "count()" will always return
        // a single row.
        return newDataset().filter(filter).collectAsList().size();
    }
}
