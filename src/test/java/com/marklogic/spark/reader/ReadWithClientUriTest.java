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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        List<Row> rows = readRowsWithClientUri(String.format(
            "%s:%s@%s:%d/spark-test-test-content",
            TEST_USERNAME, TEST_PASSWORD, testConfig.getHost(), testConfig.getRestPort()
        ));
        assertEquals(15, rows.size());
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
