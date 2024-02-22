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
