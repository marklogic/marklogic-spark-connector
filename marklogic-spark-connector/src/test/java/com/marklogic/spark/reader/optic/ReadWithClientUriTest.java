/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReadWithClientUriTest extends AbstractIntegrationTest {

    @Test
    void validUri() {
        List<Row> rows = readRowsWithClientUri(String.format(
            "%s:%s@%s:%d",
            TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort()
        ));
        assertEquals(15, rows.size());
    }

    @Test
    void usernameAndPasswordBothRequireDecoding() {
        List<Row> rows = readRowsWithClientUri(String.format(
            "%s:%s@%s:%d",
            "spark-test-user%40", "sp%40r%3Ak", testConfig.getHost(), testConfig.getRestPort()
        ));
        assertEquals(15, rows.size(), "This is just verifying that the user's username and password are correctly " +
            "decoded. The user must encode the ':' and '@' values so that they can be included in the client.uri " +
            "value.");
    }

    @Test
    void uriWithDatabase() {
        String clientUri = String.format(
            "%s:%s@%s:%d/spark-test-test-content",
            TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort()
        );

        List<Row> rows = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, clientUri)
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', '').select([op.col('LastName'), op.col('rowID')])")
            .schema(new StructType()
                .add("LastName", DataTypes.StringType)
                .add("rowid", DataTypes.StringType)
            )
            .load()
            .collectAsList();

        // Find last names that erroneously appear twice in the result set.
        Map<String, String> lastNamesAndRowIds = new HashMap<>();
        List<String> duplicateLastNames = new ArrayList<>();
        rows.forEach(row -> {
            String lastName = row.getString(0);
            String rowId = row.getString(1);
            if (lastNamesAndRowIds.containsKey(lastName)) {
                duplicateLastNames.add(lastName + "-" + lastNamesAndRowIds.get(lastName));
                duplicateLastNames.add(lastName + "-" + rowId);
            } else {
                lastNamesAndRowIds.put(lastName, rowId);
            }
        });

        assertEquals(15, rows.size(),
            "Expected 15 rows as there are 15 author documents. This is one of a couple dozen tests that " +
                "will fail when run against the 3-node cluster due to the same row being returned multiple times. " +
                "This does not happen when running against a single-node cluster. Additionally, if the data is " +
                "reloaded, the problem will not happen again until the full test suite is run again. " +
                "List of duplicate last names with their row IDs: " + duplicateLastNames);
    }

    @Test
    void uriWithInvalidDatabase() {
        String uri = String.format(
            "%s:%s@%s:%d/database-doesnt-exist",
            TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort()
        );

        RuntimeException ex = assertThrows(RuntimeException.class, () -> readRowsWithClientUri(uri));
        assertTrue(ex.getMessage().contains("XDMP-NOSUCHDB: No such database database-doesnt-exist"),
            "Unexpected error: " + ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "has no 'at' symbol",
        "user@host@port",
        "user@host:port",
        "user:password@host",
        "user:password:something@host:port",
        "user:password@host:port:something"
    })
    void invalidConnectionString(String connectionString) {
        ConnectorException ex = assertThrows(ConnectorException.class, () -> readRowsWithClientUri(connectionString));
        assertEquals(
            "Unable to connect to MarkLogic; cause: Invalid value for connection string; must be username:password@host:port/optionalDatabaseName",
            ex.getMessage()
        );
    }

    @Test
    void nonNumericPort() {
        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> readRowsWithClientUri("user:password@host:nonNumericPort"));
        assertEquals("Unable to connect to MarkLogic; cause: Invalid value for connection string; port must be numeric, but was 'nonNumericPort'",
            ex.getMessage());
    }

    @Test
    void nonNumericPortWithDatabase() {
        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> readRowsWithClientUri("user:password@host:nonNumericPort/database"));
        assertEquals("Unable to connect to MarkLogic; cause: Invalid value for connection string; port must be numeric, but was 'nonNumericPort'",
            ex.getMessage());
    }

    private List<Row> readRowsWithClientUri(String clientUri) {
        return newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, clientUri)
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors')")
            .load()
            .collectAsList();
    }
}
