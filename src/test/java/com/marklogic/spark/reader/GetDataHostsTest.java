package com.marklogic.spark.reader;

import com.marklogic.mgmt.ManageClient;
import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.resource.hosts.HostManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetDataHostsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        final List<String> expectedHostNames = getExpectedHostNames();

        List<HostUtil.DataHost> dataHosts = HostUtil.getDataHosts(getDatabaseClient());
        assertEquals(expectedHostNames.size(), dataHosts.size(), "Expecting each host in the cluster to be returned, " +
            "assuming that each has a forest with the associated database.");

        final String expectedBootstrapHostName = testConfig.getHost();
        assertEquals(expectedBootstrapHostName, dataHosts.get(0).hostName,
            "The first host in the list is expected to be the 'bootstrap' host and should have a 'request host' name " +
                "equal to that of the 'mlHost' property that our test plumbing uses to connect to MarkLogic. " +
                "The code that was copied from the Java Client to determine hosts will prefer using the 'request host' " +
                "name as opposed to the actual MarkLogic host name.");
    }

    private List<String> getExpectedHostNames() {
        ManageConfig config = new ManageConfig(
            testConfig.getHost(), 8002, testConfig.getUsername(), testConfig.getPassword()
        );
        return new HostManager(new ManageClient(config)).getHostNames();
    }
}
