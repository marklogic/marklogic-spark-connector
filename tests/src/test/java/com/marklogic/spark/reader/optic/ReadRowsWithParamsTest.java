/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that a user can declare the existence of Optic params with default values in an Optic query and then push
 * values to those params via the Spark filter operation, instead of having the Spark filter operation push the value
 * down in an Optic "where" operation.
 */
class ReadRowsWithParamsTest extends AbstractIntegrationTest {

    @Test
    void defaultStringParam() {
        String query = """
            op.fromView('Medical', 'Authors', '')
                .where(op.eq(op.col('LastName'), op.param('myValue')))
            """;

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "Shoebotham")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Shoebotham", rows.get(0).getAs("LastName"),
            "The default param value of Shoebotham should be used to filter the Optic query.");
    }

    @Test
    void filteredStringParam() {
        String query = """
            op.fromView('Medical', 'Authors', '')
                .where(op.eq(op.col('LastName'), op.param('myValue')))
            """;

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "Shoebotham")
            .load()
            .filter("myValue == 'Wooles'")
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Wooles", rows.get(0).getAs("LastName"),
            "The default value of Shoebotham should be overridden via the Spark filter call.");
    }

    @Test
    void intParam() {
        String query = """
            op.fromView('Medical', 'Authors', '')
                .where(op.eq(op.col('LuckyNumber'), op.param('myValue')))
            """;

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "13")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Wooles", rows.get(0).getAs("LastName"),
            "Verifies that a non-string param can be used. The type does not need to be defined because the user is " +
                "not performing any filter operations on it.");
    }

    @Test
    void filteredIntParam() {
        String query = """
            op.fromView('Medical', 'Authors', '')
                .where(op.eq(op.col('LuckyNumber'), op.param('myValue')))
            """;

        List<Row> rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "13")
            .option(Options.READ_OPTIC_PARAM_TYPE_PREFIX + "myValue", "integer")
            .load()
            .filter("myValue = 2")
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Bernadzki", rows.get(0).getAs("LastName"),
            "Verifies that if the user performs a filter operation on the op.param, the type must be defined. In " +
                "this case, the type is an integer, which allows for the 'myValue = 2' filter expression to be " +
                "accepted by Spark.");
    }
}
