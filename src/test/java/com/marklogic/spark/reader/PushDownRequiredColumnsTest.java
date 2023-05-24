package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * There's not a great way to verify that what MarkLogic is returning only has the required columns. We'd need to add
 * a test-only hook that captures the actual JSON, as opposed to the current hook that only fires when the partition
 * reader has "close()" called. Checking on that test-only hook for every single row seems potentially excessive.
 * <p>
 * So this test verifies that "select" works in various scenarios, but really need to check the logging to verify that
 * MarkLogic is only returning the selected columns.
 */
public class PushDownRequiredColumnsTest extends AbstractPushDownTest {

    @Test
    void withNoQualifier() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, QUERY_WITH_NO_QUALIFIER)
            .load()
            .orderBy("ForeName")
            .select("ForeName", "LastName")
            .collectAsList();

        assertEquals(15, rows.size());
        assertEquals("Aida", rows.get(0).getAs("ForeName"));
        assertEquals("Humbee", rows.get(0).getAs("LastName"));
    }

    @Test
    void withSchemaAndViewQualifiers() {
        List<Row> rows = newDefaultReader()
            .load()
            .orderBy("`Medical.Authors.ForeName`")
            .select("`Medical.Authors.ForeName`", "`Medical.Authors.LastName`")
            .collectAsList();

        assertEquals(15, rows.size());
        assertEquals("Aida", rows.get(0).getAs("Medical.Authors.ForeName"));
        assertEquals("Humbee", rows.get(0).getAs("Medical.Authors.LastName"));
    }

    @Test
    void withViewQualifier() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, "op.fromView('Medical', 'Authors', 'hey')")
            .load()
            .orderBy("`hey.ForeName`")
            .select("`hey.ForeName`", "`hey.LastName`")
            .collectAsList();

        assertEquals(15, rows.size());
        assertEquals("Aida", rows.get(0).getAs("hey.ForeName"));
        assertEquals("Humbee", rows.get(0).getAs("hey.LastName"));
    }

    @Test
    void dropColumns() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, QUERY_WITH_NO_QUALIFIER)
            .load()
            .orderBy("ForeName")
            .drop("CitationID", "LastName")
            .collectAsList();

        assertEquals(15, rows.size());

        Row row = rows.get(0);
        assertEquals("Aida", row.getString(0), "ForeName should be the first column since CitationID and " +
            "LastName were dropped");
        assertThrows(IllegalArgumentException.class, () -> row.getAs("CitationID"));
        assertThrows(IllegalArgumentException.class, () -> row.getAs("LastName"));
    }
}
