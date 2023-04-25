package com.marklogic.spark.reader;

import com.marklogic.mgmt.ManageClient;
import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.resource.hosts.HostManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetDataHostsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        final List<String> expectedHostNames = getExpectedHostNames();

        List<HostUtil.DataHost> dataHosts = HostUtil.getDataHosts(getDatabaseClient());
        assertEquals(expectedHostNames.size(), dataHosts.size(), "Expecting each host in the cluster to be returned, " +
            "assuming that each has a forest with the associated database.");

        List<String> actualHostNames = dataHosts.stream().map(host -> host.hostName).collect(Collectors.toList());
        Collections.sort(actualHostNames);

        final String expectedBootstrapHostName = testConfig.getHost();
        assertTrue(actualHostNames.contains(expectedBootstrapHostName),
            "The host name used to connect to MarkLogic is expected to be in the list of actual host names. " +
                "The code that was copied from the Java Client to determine hosts will prefer using the 'request host' " +
                "name as opposed to the actual MarkLogic host name. " + actualHostNames);

        if (actualHostNames.size() > 1) {
            final String message = "Unexpected host names: " + actualHostNames;
            assertTrue(actualHostNames.contains("node2.local"), message);
            assertTrue(actualHostNames.contains("node3.local"), message);
        }
    }

    private List<String> getExpectedHostNames() {
        ManageConfig config = new ManageConfig(
            testConfig.getHost(), 8002, testConfig.getUsername(), testConfig.getPassword()
        );
        return new HostManager(new ManageClient(config)).getHostNames();
    }
}
