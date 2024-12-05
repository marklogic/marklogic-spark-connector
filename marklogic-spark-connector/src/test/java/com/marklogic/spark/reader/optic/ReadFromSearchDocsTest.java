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

class ReadFromSearchDocsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_OPTIC_QUERY, "op.fromSearchDocs(cts.collectionQuery('author'))")
            .option(Options.READ_BATCH_SIZE, 0)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.CLIENT_URI, makeClientUri())
            .load()
            .limit(15)
            .collectAsList();

        assertEquals(15, rows.size());
    }
}
