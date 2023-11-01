package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadWithCustomCodeTest extends AbstractIntegrationTest {

    @Test
    void evalJavaScript() {
        List<Row> rows = readRows(Options.READ_JAVASCRIPT, "Sequence.from(['firstValue', 'secondValue'])");

        assertEquals(2, rows.size());
        assertEquals("firstValue", rows.get(0).getString(0));
        assertEquals("secondValue", rows.get(1).getString(0));
        verifyUriSchemaIsUsed(rows);
    }

    @Test
    void evalXQuery() {
        List<Row> rows = readRows(Options.READ_XQUERY, "(1,2,3)");

        assertEquals(3, rows.size(), "Expected 3 rows; actual rows: " + rowsToString(rows));
        assertEquals("1", rows.get(0).getString(0));
        assertEquals("2", rows.get(1).getString(0));
        assertEquals("3", rows.get(2).getString(0));
        verifyUriSchemaIsUsed(rows);
    }

    @Test
    void invokeJavaScript() {
        List<Row> rows = readRows(Options.READ_INVOKE, "/getAuthors.sjs");

        assertEquals(2, rows.size());
        assertEquals("/author1.json", rows.get(0).getString(0));
        assertEquals("/author2.json", rows.get(1).getString(0));
        verifyUriSchemaIsUsed(rows);
    }

    @Test
    void invokeXQuery() {
        List<Row> rows = readRows(Options.READ_INVOKE, "/getAuthors.xqy");

        assertEquals(2, rows.size());
        assertEquals("/author1.xml", rows.get(0).getString(0));
        assertEquals("/author2.xml", rows.get(1).getString(0));
        verifyUriSchemaIsUsed(rows);
    }

    /**
     * Demonstrates how a user's custom code can return any kind of JSON object, as long as the schema is
     * defined to match those objects.
     */
    @Test
    void customSchema() {
        List<Row> rows = startRead()
            .option(Options.READ_INVOKE, "/getAuthorObjects.sjs")
            .schema(new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
            )
            .load()
            .collectAsList();

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("Jane", rows.get(0).getString(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("John", rows.get(1).getString(1));
    }

    private List<Row> readRows(String option, String value) {
        return startRead()
            .option(option, value)
            .load()
            .collectAsList();
    }

    private DataFrameReader startRead() {
        return newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

    private void verifyUriSchemaIsUsed(List<Row> rows) {
        rows.forEach(row -> {
            StructType schema = row.schema();
            assertEquals(1, schema.fields().length);
            StructField field = schema.fields()[0];
            assertEquals("URI", field.name());
            assertEquals(DataTypes.StringType, field.dataType());
        });
    }
}

