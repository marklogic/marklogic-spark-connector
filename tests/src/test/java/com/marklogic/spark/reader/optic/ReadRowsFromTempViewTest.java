/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadRowsFromTempViewTest extends AbstractIntegrationTest {

    /**
     * Demonstrates that Spark's sql method can be used against a temporary Spark view based on our connector. This is
     * not intended to demonstrate that every possible SQL clause works - though the assumption is that a SQL query
     * is translated into an expected set of Spark filter classes that may be pushed down to our connector.
     */
    @Test
    void tempView() {
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', '')")
            .load()
            .createOrReplaceTempView("Author");

        assertEquals(15, sparkSession.sql("select * from Author").count(), "All 15 author rows should " +
            "be accessible via Spark's sql method against the temp view.");

        List<Row> rows = sparkSession.sql("select * from Author where Base64Value is not null").collectAsList();
        assertEquals(1, rows.size(), "The 'is not null' should get pushed down to our connector via the temp view so " +
            "that only the 1 row with a Base64Value is selected.");
        assertEquals(1, rows.get(0).getLong(0));
        assertEquals("Golby", rows.get(0).getString(1));

        rows = sparkSession.sql("select * from Author where Base64Value is not null and CitationID = 2").collectAsList();
        assertEquals(0, rows.size(), "No rows are expected since the only row with a Base64Value has a CitationID of 1.");
    }

    @Test
    void sqlWithLocate() {
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', 'test')")
            .load()
            .createOrReplaceTempView("Author");

        List<Row> rows = sparkSession
            .sql("select `test.CitationID`, `test.LastName` from Author where locate('umb', `test.LastName`) >= 1")
            .collectAsList();
        assertEquals(1, rows.size());
        assertEquals(5, rows.get(0).getLong(0));
        assertEquals("Humbee", rows.get(0).getString(1));
    }


}
