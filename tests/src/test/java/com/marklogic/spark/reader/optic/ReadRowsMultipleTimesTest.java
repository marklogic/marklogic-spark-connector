/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadRowsMultipleTimesTest extends AbstractIntegrationTest {

    /**
     * Log statements are included here so it's easy to see what classes get created based on different Spark API calls.
     */
    @Test
    void twoReadsWithInsertInBetween() {
        logger.info("Creating reader");
        Dataset<Row> dataset = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('sparkTest', 'allTypes')")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();

        logger.info("Calling count() the first time");
        assertEquals(3, dataset.count());

        insertDocThatProjectsASecondRow();

        logger.info("Calling count() the second time");
        assertEquals(4, dataset.count(), "Because a 'load' in Spark doesn't actually load data, the " +
            "user expectation is that when a Spark function is called that does force data to be loaded, the " +
            "data should be loaded from MarkLogic at that particular timestamp, not when the dataset was first " +
            "created.");

        logger.info("Finished with test");
    }

    private void insertDocThatProjectsASecondRow() {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.putArray("allTypes").addObject().put("intValue", 10);
        getDatabaseClient().newJSONDocumentManager().write("/allTypes2.json",
            TestUtil.withDefaultPermissions(new DocumentMetadataHandle()),
            new JacksonHandle(doc));
    }
}
