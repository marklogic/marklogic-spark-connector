/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushDownFilterTest extends AbstractPushDownTest {

    /**
     * equalTo has several tests to verify that filter/where work the same (or at least appear to) and they can be
     * combined as well. No need to re-test that for every other filter type.
     */
    @Test
    void equalToWithFilter() {
        assertEquals(4, getCountOfRowsWithFilter("CitationID == 1"));
        assertEquals(4, countOfRowsReadFromMarkLogic);
    }

    @Test
    void equalToWithSchemaAndViewQualifier() {
        assertEquals(4, newDefaultReader()
            .load()
            .filter("`Medical.Authors.CitationID` == 1")
            .collectAsList()
            .size(), "Verifying that a filter with a fully-qualified column name still works correctly.");
        assertEquals(4, countOfRowsReadFromMarkLogic);
    }

    @Test
    void equalToWithViewQualifier() {
        assertEquals(4, newDefaultReader()
            .option(Options.READ_OPTIC_DSL, "op.fromView('Medical', 'Authors', 'myView')")
            .load()
            .filter("`myView.CitationID` == 1")
            .collectAsList()
            .size(), "Verifying that a filter with a view-qualified column name still works correctly.");
        assertEquals(4, countOfRowsReadFromMarkLogic);
    }

    @Test
    void noRowsFound() {
        assertEquals(0, newDefaultReader()
            .option(Options.READ_OPTIC_DSL, NO_AUTHORS_QUERY)
            .load()
            .filter("CitationID == 1")
            .collectAsList()
            .size());
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void equalToWithWhere() {
        assertEquals(2, getCountOfRowsWithFilter("CitationID = 5"));
        assertEquals(2, countOfRowsReadFromMarkLogic);
    }

    @Test
    void equalToWithString() {
        assertEquals(0, getCountOfRowsWithFilter("LastName == 'No match'"));
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void equalToWithWhereAndFilter() {
        assertEquals(1, newDataset().where("CitationID = 1").filter("LastName == 'Golby'").count());
        assertEquals(1, countOfRowsReadFromMarkLogic);
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
        assertEquals(3, countOfRowsReadFromMarkLogic);
    }

    @Test
    void greaterThanOrEqual() {
        assertEquals(7, getCountOfRowsWithFilter("CitationID >= 3"));
        assertEquals(7, countOfRowsReadFromMarkLogic);
    }

    @Test
    void lessThan() {
        assertEquals(4, getCountOfRowsWithFilter("CitationID < 2"));
        assertEquals(4, countOfRowsReadFromMarkLogic);
    }

    @Test
    void lessThanOrEqual() {
        assertEquals(8, getCountOfRowsWithFilter("CitationID <= 2"));
        assertEquals(8, countOfRowsReadFromMarkLogic);
    }

    /**
     * This doesn't result in an "And" filter being created; Spark just passes in two EqualTo filters that are
     * naturally AND'ed together. The orWithAnd test requires that an "And" filter be supported.
     */
    @Test
    void and() {
        assertEquals(9, getCountOfRowsWithFilter("CitationID < 5 AND CitationID > 1"));
        assertEquals(9, countOfRowsReadFromMarkLogic);
    }

    @Test
    void or() {
        assertEquals(8, getCountOfRowsWithFilter("CitationID == 1 OR CitationID == 2"));
        assertEquals(8, countOfRowsReadFromMarkLogic);
    }

    @Test
    void andWithinOr() {
        // This actually results in an "and" filter being created.
        assertEquals(5, getCountOfRowsWithFilter("(CitationID < 3 AND CitationID > 1) OR CitationID == 4"));
        assertEquals(5, countOfRowsReadFromMarkLogic,
            "Expecting 4 authors with a CitationID of 2 and 1 with a CitationID of 4.");
    }

    @Test
    void not() {
        assertEquals(11, getCountOfRowsWithFilter("CitationID != 1"));
        assertEquals(11, countOfRowsReadFromMarkLogic);
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
        assertEquals(7, countOfRowsReadFromMarkLogic);
    }

    @Test
    void inWithNoMatches() {
        assertEquals(0, getCountOfRowsWithFilter("LastName in ('Doesnt', 'Match', 'Anything')"));
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void isNotNull() {
        assertEquals(2, newDataset().filter(new Column("BooleanValue").isNotNull()).collectAsList().size());
        assertEquals(2, countOfRowsReadFromMarkLogic);
    }

    @Test
    void isNotNullQualified() {
        assertEquals(2, newDefaultReader()
            .load()
            .filter(new Column("`Medical.Authors.BooleanValue`").isNotNull())
            .collectAsList()
            .size());

        assertEquals(2, countOfRowsReadFromMarkLogic,
            "2 of the authors are expected to have a BooleanValue column.");
    }

    @Test
    void isNull() {
        assertEquals(13, newDataset()
            .filter(new Column("BooleanValue").isNull())
            .collectAsList()
            .size());
        assertEquals(13, countOfRowsReadFromMarkLogic,
            "13 of the authors are expected to have a null BooleanValue column.");
    }

    @Test
    void isNullQualified() {
        assertEquals(13, newDefaultReader()
            .load()
            .filter(new Column("`Medical.Authors.BooleanValue`").isNull())
            .collectAsList().size());
        assertEquals(13, countOfRowsReadFromMarkLogic);
    }

    @Test
    void stringContains() {
        List<Row> rows = newDataset().filter(new Column("LastName").contains("umbe")).collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1, countOfRowsReadFromMarkLogic);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringContainsNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").contains("umee")).collectAsList().size());
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void stringStartsWith() {
        List<Row> rows = newDataset().filter(new Column("LastName").startsWith("Humb")).collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1, countOfRowsReadFromMarkLogic);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringStartsWithNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").startsWith("umbe")).collectAsList().size());
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    @Test
    void stringEndsWith() {
        List<Row> rows = newDataset().filter(new Column("LastName").endsWith("bee")).collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1, countOfRowsReadFromMarkLogic);
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void stringEndsWithNoMatch() {
        assertEquals(0, newDataset().filter(new Column("LastName").endsWith("umbe")).collectAsList().size());
        assertEquals(0, countOfRowsReadFromMarkLogic);
    }

    private Dataset<Row> newDataset() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_DSL, QUERY_WITH_NO_QUALIFIER)
            // Use a single call to MarkLogic so it's easier to verify from the logging
            // that only N rows were returned.
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();
    }

    private long getCountOfRowsWithFilter(String filter) {
        // collectAsList is used here so we can count how many rows are returned, as "count()" will always return
        // a single row.
        return newDataset().filter(filter).collectAsList().size();
    }
}
