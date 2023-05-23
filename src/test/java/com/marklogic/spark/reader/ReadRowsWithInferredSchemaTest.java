package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Getting some odd behavior with this class where if it runs in a suite after any of the "write" tests, then the
// queries involving sparkTest.allTypes will not return any data. Have not figured out why that is. Running this test
// first or by itself produces a successful test.
@Order(1)
public class ReadRowsWithInferredSchemaTest extends AbstractIntegrationTest {

    @Test
    void allTypes() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 1'))")
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
        assertEquals("http://example.org/", row.getString(17)); // anyURI
        if (isMarkLogic10()) {
            assertNull(row.getString(18)); // point
            assertNull(row.getString(19)); // longLatPoint
        } else {
            assertEquals("50,50", row.getString(18)); // point
            assertEquals("50,50", row.getString(19)); // longLatPoint
        }
        assertTrue(row.getBoolean(20));
        assertEquals("c2xpbmdzIGFuZCBhcnJvd3Mgb2Ygb3V0cmFnZW91cyBmb3J0dW5l", row.getString(21)); // base64Binary
        assertEquals("499602D2", row.getString(22)); // hexBinary
        assertEquals("1", row.getString(23), "Because MarkLogic defines the type of 'byte' as 'none', the Spark " +
            "connector treats it as a string."); // byte
        assertEquals("PT1M", row.getString(24)); // duration
        assertEquals("--04-18", row.getString(25)); // gMonthDay
        assertEquals(1, row.getInt(26));
        assertEquals(-1, row.getInt(27)); // negativeInteger
        assertEquals(11, row.getInt(28)); // nonNegativeInteger
        assertEquals(-11, row.getInt(29)); // nonPositiveInteger
        assertEquals(20, row.getInt(30)); // positiveInteger
        assertEquals(7, row.getInt(31)); // short
        assertEquals(4, row.getInt(32)); // unsignedByte
        assertEquals(8, row.getInt(33)); // unsignedShort
        assertEquals("http://example.org/", row.getString(34)); // IRI
    }

    @Test
    void allColumnsNullExceptRequiredOne() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 2'))")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals(35, row.size(), "Expecting all 35 columns to still exist, even though all but one have a null value");
        assertEquals(2, row.getInt(0));
        for (int i = 1; i < 35; i++) {
            assertNull(row.get(i));
        }
    }

    @Test
    void rowWithInvalidLongValueThatShouldBeIgnored() {
        List<Row> rows = newDefaultReader()
            .option(Options.READ_OPTIC_DSL,
                "op.fromView('sparkTest', 'allTypes').where(op.sqlCondition('intValue = 3'))")
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
            .option(Options.READ_OPTIC_DSL,
                "op.fromView('sparkTest', 'allTypes')" +
                    ".select(['intValue', 'timeValue'])")
            .option(Options.READ_NUM_PARTITIONS, "1")
            .load()
            .collectAsList();

        assertEquals(3, rows.size());
        rows.forEach(row ->
            assertEquals(2, row.size(), "Should just have the two columns returned by the Optic query"));
    }

}
