package com.marklogic.spark.reader.optic;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadFromNonRestApiServerTest extends AbstractIntegrationTest {

    /**
     * Verifies that when a user connects to a non-REST-API server, we give them something more helpful than just a
     * 404.
     */
    @Test
    void test() {
        DataFrameReader reader = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .option(Options.CLIENT_URI, String.format("admin:admin@%s:8001", testConfig.getHost()));

        ConnectorException ex = assertThrows(ConnectorException.class, () -> reader.load());
        assertEquals("Unable to connect to MarkLogic; status code: 404; ensure that " +
            "you are attempting to connect to a MarkLogic REST API app server. See the MarkLogic documentation on " +
            "REST API app servers for more information.", ex.getMessage());
    }
}
