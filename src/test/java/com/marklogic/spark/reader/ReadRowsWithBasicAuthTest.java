package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.mgmt.ManageClient;
import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.resource.appservers.ServerManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadRowsWithBasicAuthTest extends AbstractIntegrationTest {

    private com.marklogic.mgmt.ManageClient manageClient;

    @BeforeEach
    void changeAuthToBasic() {
        manageClient = new ManageClient(new ManageConfig(testConfig.getHost(), 8002,
            testConfig.getUsername(), testConfig.getPassword()));
        setServerAuthentication("basic");
    }

    @AfterEach
    void changeAuthToDigest() {
        setServerAuthentication("digest");
    }

    /**
     * Verifies that case-sensitive options, like "authType", work properly. Spark lower-cases options by default, but
     * makes a case-sensitive version of them available to the connector. This verifies that our connector uses the
     * case-sensitive version.
     */
    @Test
    void test() {
        long count = newDefaultReader()
            .option("spark.marklogic.client.authType", "basic")
            .load()
            .count();

        assertEquals(15, count);
    }

    private void setServerAuthentication(String value) {
        new ServerManager(manageClient).save(new ObjectMapper().createObjectNode()
            .put("server-name", "spark-test-test")
            .put("group-name", "Default")
            .put("authentication", value)
            .toString());
    }
}
