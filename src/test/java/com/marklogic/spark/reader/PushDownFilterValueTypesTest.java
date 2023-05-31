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
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that each of the types supported by the bindParam method in the Java Client is handled correctly.
 * <p>
 * These tests verify that a query works and the correct row is returned, thus assuming that the bindParam method
 * was called correctly. To further verify that, run these tests with a debugger and checking the logic in
 * SingleValueFilter to see which bindParam method is actually called.
 */
public class PushDownFilterValueTypesTest extends AbstractIntegrationTest {

    @Test
    void doubleValue() {
        verifyOneRowReturned("doubleValue == 2.2");
        verifyOneRowReturned("decimalValue == 3.3");
    }

    @Test
    void floatValue() {
        verifyOneRowReturned("floatValue == 1.0");
    }

    @Test
    void longValue() {
        verifyOneRowReturned("longValue == 3");
        verifyOneRowReturned("unsignedLongValue == 4");
    }

    @Test
    void intValue() {
        verifyOneRowReturned("intValue == 1");
        verifyOneRowReturned("unsignedIntValue == 2");
    }

    @Test
    void shortValue() {
        // Both of these result in an "int" value, as the server columnInfo for a "short" results in a type of "int".
        verifyOneRowReturned("shortValue == 7");
        assertEquals(1, newDataset().filter(new Column("shortValue").equalTo((short) 7)).count());
    }

    @Test
    void byteValue() {
        // Because byteValue has a type of "none" from the server, the below filters result in an IsNotNull instead of
        // an EqualTo.
        verifyOneRowReturned("byteValue == 1");
        assertEquals(1, newDataset().filter(new Column("byteValue").equalTo((byte) 1)).count());
    }

    @Test
    void booleanValue() {
        verifyOneRowReturned("booleanValue == true");
    }

    @Test
    void stringValue() {
        verifyOneRowReturned("stringValue == 'hello'");
    }

    private void verifyOneRowReturned(String filter) {
        assertEquals(1, newDataset().filter(filter).count());
    }

    private Dataset<Row> newDataset() {
        return newDefaultReader()
            .option(Options.READ_OPTIC_DSL, "op.fromView('sparkTest', 'allTypes', '')")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_BATCH_SIZE, 0)
            .load();
    }

}
