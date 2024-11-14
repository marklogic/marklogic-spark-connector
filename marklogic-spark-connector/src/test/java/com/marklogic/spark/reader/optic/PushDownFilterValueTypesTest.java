/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.RequiresMarkLogic11OrLower;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that each of the types supported by the bindParam method in the Java Client is handled correctly.
 * <p>
 * These tests verify that a query works and the correct row is returned, thus assuming that the bindParam method
 * was called correctly. To further verify that, run these tests with a debugger and checking the logic in
 * SingleValueFilter to see which bindParam method is actually called.
 * <p>
 * As of 2024-10-29, this is mysteriously failing with 8016 connection issues on Jenkins. Does not fail on MarkLogic
 * 11 though. Will investigate more soon.
 */
@ExtendWith(RequiresMarkLogic11OrLower.class)
class PushDownFilterValueTypesTest extends AbstractIntegrationTest {

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
            .option(Options.READ_OPTIC_QUERY, "op.fromView('sparkTest', 'allTypes', '')")
            .load();
    }

}
