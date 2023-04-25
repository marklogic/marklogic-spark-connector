package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * For more information on Spark data types, see https://spark.apache.org/docs/latest/sql-ref-datatypes.html.
 * <p>
 * This test does not cover int/string, as those are covered by ReadRowsTest.
 */
public class ReadRowsWithAllSparkDataTypesTest extends AbstractIntegrationTest {

    @Test
    void floatType() {
        readRowsWithCitationIDType(DataTypes.FloatType).forEach(row -> {
            float id = row.getFloat(0);
            assertTrue(id >= 1 && id <= 5);
        });
    }

    @Test
    void doubleType() {
        readRowsWithCitationIDType(DataTypes.DoubleType).forEach(row -> {
            double id = row.getDouble(0);
            assertTrue(id >= 1 && id <= 5);
        });
    }

    @Test
    void longType() {
        readRowsWithCitationIDType(DataTypes.LongType).forEach(row -> {
            long id = row.getLong(0);
            assertTrue(id >= 1 && id <= 5);
        });
    }

    @Test
    void shortType() {
        readRowsWithCitationIDType(DataTypes.ShortType).forEach(row -> {
            short id = row.getShort(0);
            assertTrue(id >= 1 && id <= 5);
        });
    }

    /**
     * Per https://spark.apache.org/docs/latest/sql-ref-datatypes.html, byte is for numbers from -128 to 127.
     */
    @Test
    void byteType() {
        readRowsWithCitationIDType(DataTypes.ByteType).forEach(row -> {
            byte id = row.getByte(0);
            assertTrue(id >= 1 && id <= 5);
        });
    }

    @Test
    void nullType() {
        readRowsWithCitationIDType(DataTypes.NullType).forEach(row ->
            assertNull(row.get(0), "Not clear on the use case here, but if the user specifies the type as 'null', " +
                "then Spark will dutifully return 'null' as the value; actual value: " + row.get(0))
        );
    }

    @Test
    void binaryType() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors').where(op.sqlCondition(\"ForeName = 'Pen'\"))")
            .schema(new StructType()
                .add("Medical.Authors.Base64Value", DataTypes.BinaryType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        Object value = rows.get(0).get(0);
        assertTrue(value instanceof byte[], "The base64 encoded value should be indexed as a base64Binary in the " +
            "TDE, which can then be casted to Spark's BinaryType; actual type: " + value.getClass());
    }

    @Test
    void trueBooleanValue() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors').where(op.sqlCondition(\"ForeName = 'Pen'\"))")
            .schema(new StructType()
                .add("Medical.Authors.BooleanValue", DataTypes.BooleanType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).getBoolean(0));
    }

    @Test
    void falseBooleanValue() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors')" +
                ".where(op.sqlCondition(\"ForeName = 'Cherianne'\"))")
            .schema(new StructType()
                .add("Medical.Authors.BooleanValue", DataTypes.BooleanType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertFalse(rows.get(0).getBoolean(0));
    }

    @Test
    void dateType() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors')" +
                ".where(op.sqlCondition(\"ForeName = 'Finlay'\"))")
            .schema(new StructType()
                .add("Medical.Authors.Date", DataTypes.DateType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        Object value = rows.get(0).get(0);
        assertTrue(value instanceof java.sql.Date);
        assertEquals("2022-07-13", value.toString());
    }

    @Test
    void timestampType() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors')" +
                ".where(op.sqlCondition(\"ForeName = 'Finlay'\"))")
            .schema(new StructType()
                .add("Medical.Authors.DateTime", DataTypes.TimestampType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        Object value = rows.get(0).get(0);
        assertTrue(value instanceof java.sql.Timestamp);
        // TODO Improve this with DEVEXP-388
        assertTrue(value.toString().startsWith("2022-07-13"),
            "Unexpected value: " + value.toString());
//        assertEquals("2022-07-13 05:00:00.0", value.toString(),
//            "The value is translated to Zulu time since that's the default time zone ID.");
    }

    /**
     * Couldn't find much in the way of docs for this type; the JIRA ticket has some sub-tasks with PR's that at least
     * provide examples - https://issues.apache.org/jira/browse/SPARK-8943 .
     */
    @Test
    void calendarIntervalType() {
        List<Row> rows = newDefaultReader()
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical', 'Authors')" +
                ".where(op.sqlCondition(\"ForeName = 'Pen'\"))")
            .schema(new StructType()
                .add("Medical.Authors.LastName", DataTypes.StringType)
                .add("Medical.Authors.CalendarInterval", DataTypes.CalendarIntervalType)
            )
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("Golby", rows.get(0).get(0));
        assertEquals("2 years 4 months", rows.get(0).get(1).toString());
    }

    private List<Row> readRowsWithCitationIDType(DataType dataType) {
        List<Row> rows = newDefaultReader()
            .schema(new StructType()
                .add("Medical.Authors.CitationID", dataType)
            )
            .load()
            .collectAsList();
        assertEquals(15, rows.size());
        return rows;
    }
}
