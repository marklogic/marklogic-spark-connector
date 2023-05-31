/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadRowsMultipleTimesTest extends AbstractIntegrationTest {

    /**
     * Log statements are included here so it's easy to see what classes get created based on different Spark API calls.
     */
    @Test
    void twoReadsWithInsertInBetween() throws Exception {
        logger.info("Creating reader");
        Dataset<Row> dataset = newDefaultReader()
            .option(Options.READ_OPTIC_DSL, "op.fromView('sparkTest', 'allTypes')")
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
        ObjectNode doc = new ObjectMapper().createObjectNode();
        doc.putArray("allTypes").addObject().put("intValue", 10);
        getDatabaseClient().newJSONDocumentManager().write("/allTypes2.json",
            new DocumentMetadataHandle().withPermission("spark-user-role", DocumentMetadataHandle.Capability.READ,
                DocumentMetadataHandle.Capability.UPDATE),
            new JacksonHandle(doc));
    }
}
