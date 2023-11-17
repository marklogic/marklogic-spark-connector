package com.marklogic.spark.reader;

import com.marklogic.client.FailedRequestException;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void userDefinedVariables() {
        List<Row> rows = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_JAVASCRIPT, "Sequence.from([firstValue, secondValue])")
            .option(Options.READ_VARS_PREFIX + "firstValue", "the first value")
            .option(Options.READ_VARS_PREFIX + "secondValue", "the second value")
            .load()
            .collectAsList();

        assertEquals(2, rows.size());
        assertEquals("the first value", rows.get(0).getString(0));
        assertEquals("the second value", rows.get(1).getString(0));
    }

    @Test
    void partitionsFromJavaScript() {
        verifyRowsAreReadFromEachForest(
            Options.READ_PARTITIONS_JAVASCRIPT, "xdmp.databaseForests(xdmp.database())"
        );
    }

    @Test
    void partitionsFromXQuery() {
        verifyRowsAreReadFromEachForest(
            Options.READ_PARTITIONS_XQUERY, "xdmp:database-forests(xdmp:database())"
        );
    }

    @Test
    void partitionsFromInvoke() {
        verifyRowsAreReadFromEachForest(
            Options.READ_PARTITIONS_INVOKE, "/getForests.sjs"
        );
    }

    @Test
    void badJavascriptForPartitions() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> startRead()
            .option(Options.READ_PARTITIONS_JAVASCRIPT, "this is invalid javascript")
            .option(Options.READ_JAVASCRIPT, "const forestId = PARTITION; cts.uris(null, [], cts.collectionQuery('author'), 0, [forestId])")
            .load()
            .collectAsList());

        assertEquals("Unable to retrieve partitions", ex.getMessage());
        assertTrue(ex.getCause() instanceof FailedRequestException, "Unexpected cause: " + ex.getCause());
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

    private void verifyRowsAreReadFromEachForest(String partitionsOption, String partitionsValue) {
        List<Row> rows = startRead()
            .option(partitionsOption, partitionsValue)
            .option(Options.READ_JAVASCRIPT, "const forestId = PARTITION; cts.uris(null, [], cts.collectionQuery('author'), 0, [forestId])")
            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Expecting all 15 author URIs to be returned across all forests, " +
            "as each forest was used as a partition.");

        final List<String> uris = rows.stream().map(row -> row.getString(0)).collect(Collectors.toList());
        for (int i = 1; i <= 15; i++) {
            String expectedUri = String.format("/author/author%d.json", i);
            assertTrue(uris.contains(expectedUri), String.format("Did not find %s in %s", expectedUri, uris));
        }
        verifyUriSchemaIsUsed(rows);
    }
}

