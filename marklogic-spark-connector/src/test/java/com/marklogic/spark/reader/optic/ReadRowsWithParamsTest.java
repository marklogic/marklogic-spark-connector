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
    void test() {
        String query = "op.fromView('Medical', 'Authors', '')" +
            "  .where(op.gt(op.col('LuckyNumber'), op.param('myValue')))";

        List<Row> rows;

//        rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
//            .option(Options.CLIENT_URI, makeClientUri())
//            .option(Options.READ_OPTIC_QUERY, query)
//            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "10")
//            .load()
//            .collectAsList();
//
//        assertEquals(5, rows.size());

        rows = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_OPTIC_PARAM_PREFIX + "myValue", "10")
            .load()
            .filter("myValue ")
            .collectAsList();

        assertEquals(3, rows.size());
    }
}
