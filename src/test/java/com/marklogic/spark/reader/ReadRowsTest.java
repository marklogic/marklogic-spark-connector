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

import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReadRowsTest extends AbstractIntegrationTest {

    @Test
    void validPartitionCountAndBatchSize() {
        List<Row> rows = newDefaultReader()
            .option("marklogic.num_partitions", "3")
            .option("marklogic.batch_size", "10000")
            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Expecting all 15 rows to be returned from the view");

        rows.forEach(row -> {
            int id = row.getInt(0);
            assertTrue(id >= 1 && id <= 5, "The citation ID is expected to be the first column value, and based on our " +
                "test data, it should have a value from 1 to 5; actual value: " + id);
            assertEquals(2, row.size(), "Expecting the row to have 2 columns since the schema in newDefaultReader " +
                "only defines 2 columns.");
        });
    }

    @Test
    void queryReturnsZeroRows() {
        List<Row> rows = newDefaultReader()
            .option("marklogic.optic_dsl", "op.fromView('Medical', 'NoAuthors')")
            .load()
            .collectAsList();

        assertEquals(0, rows.size(), "When no rows exist, the /v1/rows endpoint currently (as of 11.0) throws a " +
            "fairly cryptic error. A user does not expect an error in this scenario, so the connector is expected " +
            "to catch that error and return a valid dataset with zero rows in it.");
    }

    @Test
    void invalidQuery() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> newDefaultReader()
            .option("marklogic.optic_dsl", "op.fromView('Medical', 'ViewNotFound')")
            .load()
            .count());

        assertTrue(ex.getMessage().startsWith("Unable to run Optic DSL query op.fromView('Medical', 'ViewNotFound'); cause: "),
            "If the query throws an error for any reason other than no rows being found, the error should be wrapped " +
                "in a new error with a message containing the user's query to assist with debugging; unexpected " +
                "message: " + ex.getMessage());
    }

    @Test
    void invalidCredentials() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> newDefaultReader()
            .option("marklogic.client.password", "invalid password")
            .load()
            .count());

        assertEquals("Unable to connect to MarkLogic; status code: 401; error message: Unauthorized", ex.getMessage(),
            "The connector should verify that it can connect to MarkLogic before it tries to analyze the user's query. " +
                "This allows for a more helpful error message so that the user can focus on fixing their connection and " +
                "authentication properties.");
    }

    @Test
    void nonNumericPartitionCount() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            newDefaultReader()
                .option("marklogic.num_partitions", "abc")
                .load()
                .count()
        );
        assertEquals("Value of 'marklogic.num_partitions' option must be numeric", ex.getMessage());
    }

    @Test
    void partitionCountLessThanOne() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            newDefaultReader()
                .option("marklogic.num_partitions", "0")
                .load()
                .count()
        );
        assertEquals("Value of 'marklogic.num_partitions' option must be 1 or greater", ex.getMessage());
    }

    @Test
    void nonNumericBatchSize() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            newDefaultReader()
                .option("marklogic.batch_size", "abc")
                .load()
                .count()
        );
        assertEquals("Value of 'marklogic.batch_size' option must be numeric", ex.getMessage());
    }

    @Test
    void batchSizeLessThanOne() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            newDefaultReader()
                .option("marklogic.batch_size", "0")
                .load()
                .count()
        );
        assertEquals("Value of 'marklogic.batch_size' option must be 1 or greater", ex.getMessage());
    }
}
