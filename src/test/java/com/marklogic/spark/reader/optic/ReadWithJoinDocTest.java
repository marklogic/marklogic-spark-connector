/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadWithJoinDocTest extends AbstractIntegrationTest {

    @Test
    void jsonDocuments() throws Exception {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "const idCol = op.fragmentIdCol('id'); " +
                    "op.fromView('sparkTest', 'allTypes', '', idCol)" +
                    ".where(op.sqlCondition('intValue = 1'))" +
                    ".joinDoc('doc', idCol)" +
                    ".select('doc')")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load()
            .collectAsList();

        assertEquals(1, rows.size());

        Row row = rows.get(0);
        JsonNode doc = objectMapper.readTree(row.getString(0));
        assertEquals(1, doc.get("allTypes").get(0).get("intValue").asInt(),
            "Verifying that the doc was correctly returned as a string in the Spark row, and could then be read via " +
                "Jackson into a JsonNode");
    }
}
