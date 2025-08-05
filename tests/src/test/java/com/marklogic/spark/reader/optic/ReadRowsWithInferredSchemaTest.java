/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.junit5.RequiresMarkLogic12;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

// Getting some odd behavior with this class where if it runs in a suite after any of the "write" tests, then the
// queries involving sparkTest.allTypes will not return any data. Have not figured out why that is. Running this test
// first or by itself produces a successful test.
@Order(1)
class ReadRowsWithInferredSchemaTest extends AbstractIntegrationTest {

    /**
     * On MarkLogic 10.0-9.5, longLatPoint is defined as an "int" by columnInfo. In MarkLogic 11+, it's
     * correctly identified as a "point". In MarkLogic 10, the value that's returned is oddly either null or
     * "50,50". When it's null, this test passes. When it's "50,50", Spark throws an error because that's not
     * of type "int". Due to this intermittent behavior, this test is now being skipped on MarkLogic 10, which
     * avoids the core problem of "int" being returned for longLatPoint.
     */
    @SuppressWarnings("java:S5961") // This method is easy to understand despite the number of assertions.
    @Test
    // The types have been updated for MarkLogic 12, such that this won't pass on MarkLogic 11.
    @ExtendWith(RequiresMarkLogic12.class)
    void allTypes() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 1'))")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load()
            .collectAsList();

        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals(1, row.getInt(0));
        assertEquals(2, row.getInt(1)); // unsignedInt
        assertEquals(3, row.getLong(2));
        assertEquals(4, row.getLong(3)); // unsignedLong
        assertEquals(1.0, row.getFloat(4));
        assertEquals(2.2, row.getDouble(5));
        assertEquals(3.3, row.getDouble(6));
        assertTrue(row.getTimestamp(7).toString().startsWith("2023-04-18 "),
            "Unexpected value: " + row.getTimestamp(7).toString());
        assertEquals("13:55:51", row.getString(8)); // time
        assertEquals("2023-04-18", row.getDate(9).toString());
        assertEquals("2023-04", row.getString(10)); // gYearMonth
        assertEquals("2023", row.getString(11)); // gYear
        assertEquals("--04", row.getString(12)); // gMonth
        assertEquals("---18", row.getString(13)); // gDay
        assertEquals("P2Y6M", row.getString(14)); // yearMonthDuration
        assertEquals("PT1M", row.getString(15)); // dayTimeDuration
        assertEquals("hello", row.getString(16));
        assertEquals("hello collated", row.getString(17));
        assertEquals("http://example.org/", row.getString(18)); // anyURI
        assertEquals("50,50", row.getString(19)); // point
        assertEquals("50,50", row.getString(20)); // longLatPoint
        assertTrue(row.getBoolean(21));
        assertEquals("slings and arrows of outrageous fortune", new String((byte[]) row.get(22))); // base64Binary
        byte[] hexBinaryValue = (byte[]) row.get(23); // hexBinary
        assertEquals(6, hexBinaryValue.length);
        assertEquals(Byte.parseByte("1"), row.getByte(24)); // byte
        assertEquals("PT1M", row.getString(25)); // duration
        assertEquals("--04-18", row.getString(26)); // gMonthDay
        assertEquals(1, row.getInt(27));
        assertEquals(-1, row.getInt(28)); // negativeInteger
        assertEquals(11, row.getInt(29)); // nonNegativeInteger
        assertEquals(-11, row.getInt(30)); // nonPositiveInteger
        assertEquals(20, row.getInt(31)); // positiveInteger
        assertEquals(7, row.getShort(32)); // short
        assertEquals(Byte.parseByte("4"), row.getByte(33)); // unsignedByte
        assertEquals(8, row.getShort(34)); // unsignedShort
        assertEquals("http://example.org/", row.getString(35)); // IRI
    }

    @Test
    void allColumnsNullExceptRequiredOne() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 2'))")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load()
            .collectAsList();

        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals(36, row.size(), "Expecting all 36 columns to still exist, even though all but one have a null value");
        assertEquals(2, row.getInt(0));
        for (int i = 1; i < 36; i++) {
            assertNull(row.get(i));
        }
    }

    @Test
    void rowWithInvalidLongValueThatShouldBeIgnored() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 3'))")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(3, row.getInt(0));
        assertNull(row.get(2), "If a value is invalid and invalidValues for the TDE column is set to 'ignore', then " +
            "when the row is retrieved, the column's value should be null.");
    }

    @Test
    void selectSubsetOfColumns() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "op.fromView('sparkTest', 'allTypes')" +
                    ".select(['intValue', 'timeValue'])")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load()
            .collectAsList();

        assertEquals(3, rows.size());
        rows.forEach(row ->
            assertEquals(2, row.size(), "Should just have the two columns returned by the Optic query"));
    }

}
