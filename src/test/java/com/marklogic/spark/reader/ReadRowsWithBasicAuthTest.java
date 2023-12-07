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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.mgmt.ManageClient;
import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.resource.appservers.ServerManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadRowsWithBasicAuthTest extends AbstractIntegrationTest {

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
