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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ReadWithClientUriTest extends AbstractIntegrationTest {

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
        RuntimeException ex = assertThrows(RuntimeException.class, () -> readRowsWithClientUri(String.format(
            "%s:%s@%s:%d/database-doesnt-exist",
            TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort()
        )));

        assertTrue(ex.getMessage().contains("XDMP-NOSUCHDB: No such database database-doesnt-exist"),
            "Unexpected error: " + ex.getMessage());
    }

    @Test
    void missingAtSymbol() {
        verifyClientUriIsInvalid("has no 'at' symbol");
    }

    @Test
    void twoAtSymbols() {
        verifyClientUriIsInvalid("user@host@port");
    }

    @Test
    void onlyOneTokenBeforeAtSymbol() {
        verifyClientUriIsInvalid("user@host:port");
    }

    @Test
    void onlyOneTokenAfterAtSymbol() {
        verifyClientUriIsInvalid("user:password@host");
    }

    @Test
    void threeTokensBeforeAtSymbol() {
        verifyClientUriIsInvalid("user:password:something@host:port");
    }

    @Test
    void threeTokensAfterAtSymbol() {
        verifyClientUriIsInvalid("user:password@host:port:something");
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

    private void verifyClientUriIsInvalid(String clientUri) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> readRowsWithClientUri(clientUri)
        );

        assertEquals(
            "Invalid value for spark.marklogic.client.uri; must be username:password@host:port",
            ex.getMessage()
        );
    }
}
