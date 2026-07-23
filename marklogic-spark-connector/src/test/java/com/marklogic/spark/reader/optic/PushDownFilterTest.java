/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * As of 2024-10-29, this is mysteriously failing with 8016 connection issues on Jenkins. Does not fail on MarkLogic
 * 11 though. Will investigate more soon.
 */
class PushDownFilterTest extends AbstractPushDownTest {

    // Tracks URIs written by insertAuthor() so they can be removed after each test, ensuring the shared "author"
    // collection - which many other tests expect to contain exactly 15 documents - isn't permanently polluted.
    private final List<String> insertedAuthorUris = new ArrayList<>();

    @AfterEach
    void deleteInsertedAuthors() {
        if (!insertedAuthorUris.isEmpty()) {
            getDatabaseClient().newJSONDocumentManager().delete(insertedAuthorUris.toArray(new String[0]));
            insertedAuthorUris.clear();
        }
    }

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
    void stringContainsLiteralPercent() {
        insertAuthor(90009, "FIND%003_PERCENT_MATCH", "Literal", "/author/find-003-percent-match.json");
        insertAuthor(90010, "FINDX003_PERCENT_CONTROL", "Literal", "/author/find-003-percent-control.json");

        List<Row> rows = newDataset().filter(new Column("LastName").contains("FIND%003")).collectAsList();
        assertEquals(1, rows.size(), "Percent must be treated literally, not as a wildcard.");
        assertEquals("FIND%003_PERCENT_MATCH", rows.get(0).getAs("LastName"));
        assertRowsReadFromMarkLogic(1);
    }

    @Test
    void stringContainsInjectionPayloadDoesNotReturnExtraRows() {
        assertEquals(0,
            newDataset().filter(new Column("LastName").contains("x%' OR 1=1 OR name LIKE '%")).collectAsList().size());
        assertRowsReadFromMarkLogic(0,
            "An injected payload must be treated as literal text and must not create a tautology.");
    }

    @Test
    void stringContainsLiteralUnderscoreMatchesOnlyLiteral() {
        insertAuthor(90001, "FIND_003_UNDERSCORE_MATCH", "Literal", "/author/find-003-underscore-match.json");
        insertAuthor(90002, "FINDX003_UNDERSCORE_MATCH", "Literal", "/author/find-003-underscore-control.json");

        List<Row> rows = newDataset().filter(new Column("LastName").contains("FIND_003")).collectAsList();
        assertEquals(1, rows.size(), "Underscore must be treated literally, not as a wildcard.");
        assertEquals("FIND_003_UNDERSCORE_MATCH", rows.get(0).getAs("LastName"));
        assertRowsReadFromMarkLogic(1);
    }

    @Test
    void stringContainsLiteralSingleQuoteMatchesOnlyLiteral() {
        insertAuthor(90003, "O'FIND003_MATCH", "Literal", "/author/find-003-quote-match.json");
        insertAuthor(90004, "OFIND003_MATCH", "Literal", "/author/find-003-quote-control.json");

        List<Row> rows = newDataset().filter(new Column("LastName").contains("O'FIND003")).collectAsList();
        assertEquals(1, rows.size(), "Single quote must be treated literally and not break SQL condition syntax.");
        assertEquals("O'FIND003_MATCH", rows.get(0).getAs("LastName"));
        assertRowsReadFromMarkLogic(1);
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
    void stringStartsWithLiteralPercentMatchesOnlyLiteral() {
        insertAuthor(90005, "%FIND003_START", "Literal", "/author/find-003-start-percent-match.json");
        insertAuthor(90006, "XFIND003_START", "Literal", "/author/find-003-start-percent-control.json");

        List<Row> rows = newDataset().filter(new Column("LastName").startsWith("%FIND003")).collectAsList();
        assertEquals(1, rows.size(), "Percent must be treated literally in a StringStartsWith filter.");
        assertEquals("%FIND003_START", rows.get(0).getAs("LastName"));
        assertRowsReadFromMarkLogic(1);
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

    @Test
    void stringEndsWithLiteralUnderscoreMatchesOnlyLiteral() {
        insertAuthor(90007, "FIND003_END_", "Literal", "/author/find-003-end-underscore-match.json");
        insertAuthor(90008, "FIND003_ENDX", "Literal", "/author/find-003-end-underscore-control.json");

        List<Row> rows = newDataset().filter(new Column("LastName").endsWith("END_")).collectAsList();
        assertEquals(1, rows.size(), "Underscore must be treated literally in a StringEndsWith filter.");
        assertEquals("FIND003_END_", rows.get(0).getAs("LastName"));
        assertRowsReadFromMarkLogic(1);
    }

    @Test
    void injectedColumnNameInStringContainsIsRejected() {
        AnalysisException ex = assertThrows(AnalysisException.class,
            () -> newDataset().filter(new Column("`LastName OR 1=1`").contains("x")).collectAsList());
        assertTrue(ex.getMessage().contains("LastName OR 1=1"));
    }

    private void insertAuthor(int citationId, String lastName, String foreName, String uri) {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.put("CitationID", citationId);
        doc.put("LastName", lastName);
        doc.put("ForeName", foreName);
        doc.put("Date", "2022-06-10");
        doc.put("DateTime", "2022-06-10 12:00:00");
        doc.put("LuckyNumber", 13);

        DocumentMetadataHandle metadata = TestUtil.withDefaultPermissions(new DocumentMetadataHandle());
        metadata.getCollections().add("author");

        getDatabaseClient().newJSONDocumentManager().write(uri, metadata, new JacksonHandle(doc));
        insertedAuthorUris.add(uri);
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
