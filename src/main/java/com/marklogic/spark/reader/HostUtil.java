package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.datamovement.ForestConfiguration;
import com.marklogic.client.datamovement.impl.DataMovementManagerImpl;
import com.marklogic.client.row.RowManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Determines hosts for distributing load. A "data host" (or "d-node") is one with at least forest associated with the
 * database that the given Java Client points to.
 * <p>
 * Mostly copied from BatcherImpl and RowBatcherImpl in the Java Client.
 */
abstract class HostUtil {

    static class DataHost {
        final String hostName;
        final RowManager rowManager;

        DataHost(DatabaseClient client) {
            this.hostName = client.getHost();
            this.rowManager = client.newRowManager();
            // Nested values won't work with JacksonParser, so we ask for type info to not be in the rows.
            this.rowManager.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        }
    }

    static List<DataHost> getDataHosts(DatabaseClient client) {
        DataMovementManagerImpl dmm = (DataMovementManagerImpl) client.newDataMovementManager();
        ForestConfiguration forestConfig = dmm.readForestConfig();
        Set<String> hostNames = getHostNamesForForests(forestConfig.listForests());
        return hostNames
            .stream()
            .map(hostName -> new DataHost(dmm.getHostClient(hostName)))
            .collect(Collectors.toList());
    }

    private static Set<String> getHostNamesForForests(Forest[] forests) {
        Set<String> hosts = new HashSet<>();
        for (Forest forest : forests) {
            if (forest.getPreferredHost() == null) {
                throw new IllegalStateException("Hostname must not be null for any forest; forest name: " + forest.getForestName());
            }
            hosts.add(forest.getPreferredHost());
        }
        // This is copied from BatcherImpl. An example of this occurs locally, where the hostName for a forest may be
        // e.g. "macpro-1234.marklogic.com", and the requestHost is "localhost". This code will remove the former from
        // the list of hosts, presumably because the former is not accessible while the latter is.
        for (Forest forest : forests) {
            String hostName = forest.getHost();
            boolean hostNameShouldNotBeUsed =
                forest.getPreferredHostType() == Forest.HostType.REQUEST_HOST &&
                    !hostName.equalsIgnoreCase(forest.getRequestHost());
            if (hostNameShouldNotBeUsed && hosts.contains(hostName)) {
                hosts.remove(hostName);
            }
        }
        return hosts;
    }
}
