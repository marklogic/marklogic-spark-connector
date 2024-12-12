/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadRowsWithParamsTest extends AbstractIntegrationTest {

    @Test
    void defaultStringParam() {
        String query = "op.fromView('Medical', 'Authors', '')" +
            "  .where(op.eq(op.col('LastName'), op.param('myValue')))";

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "Shoebotham")
            .load()
//            .filter("myValue = 'Wooles'")
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Shoebotham", rows.get(0).getAs("LastName"));
//        rows.forEach(row -> System.out.println("Row: " + row.prettyJson()));
    }

    @Test
    void filteredStringParam() {
        String query = "op.fromView('Medical', 'Authors', '')" +
            "  .where(op.eq(op.col('LastName'), op.param('myValue')))";

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "Shoebotham")
            .load()
            .filter("myValue == 'Wooles'")
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Wooles", rows.get(0).getAs("LastName"));
    }

    @Test
    void intParam() {
        String query = "op.fromView('Medical', 'Authors', '')" +
            "  .where(op.eq(op.col('LuckyNumber'), op.param('myValue')))";

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "13")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Wooles", rows.get(0).getAs("LastName"));
    }

    @Test
    void filteredIntParam() {
        String query = "op.fromView('Medical', 'Authors', '')" +
            "  .where(op.eq(op.col('LuckyNumber'), op.param('myValue')))";

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "13")
            .load()
            .filter("myValue = 2")
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Bernadzki", rows.get(0).getAs("LastName"));
    }
}
