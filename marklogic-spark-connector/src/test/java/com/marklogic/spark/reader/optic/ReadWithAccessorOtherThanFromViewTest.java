/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that DSL queries with accessors other than "fromView" will succeed, as of the 2.5.0 release.
 */
class ReadWithAccessorOtherThanFromViewTest extends AbstractIntegrationTest {

    @Test
    void fromSearchDocs() {
        long count = readWithQuery("op.fromSearchDocs(cts.collectionQuery('author'))")
            .load()
            // Verifies that an operation that would normally be pushed down does not cause an error,
            // as no filters can be pushed down when not using fromView.
            .limit(15)
            .count();

        assertEquals(15, count);
    }

    @Test
    void fromLexicons() {
        List<Row> rows = readWithQuery("op.fromLexicons({" +
            "  'CitationID': cts.elementReference('CitationID')" +
            "})" +
            ".groupBy('CitationID', op.count('counts'))")
            .load()
            .orderBy("CitationID")
            .collectAsList();

        assertEquals(5, rows.size());

        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(4, rows.get(0).getLong(1), "Should be 4 rows with CitationID=1.");
    }

    private DataFrameReader readWithQuery(String query) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.CLIENT_URI, makeClientUri());
    }
}
