package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.MarkLogicIOException;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class ContextSupportTest extends AbstractIntegrationTest {

    private Map<String, String> options = new HashMap<>();

    /**
     * For automated tests, we only have the single-node MarkLogic cluster and thus can't verify that direct
     * connections really work. But this test at least verifies that the "host" input overrides what is in the
     * options.
     */
    @Test
    void directConnectionToHost() {
        options.put(Options.CLIENT_URI, makeClientUri());

        ContextSupport support = new ContextSupport(options);
        DatabaseClient client = support.connectToMarkLogic(testConfig.getHost());
        assertTrue(client.checkConnection().isConnected());

        MarkLogicIOException ex = assertThrows(MarkLogicIOException.class,
            () -> support.connectToMarkLogic("invalid-host"));
        assertTrue(ex.getCause() instanceof UnknownHostException);
    }

    @Test
    void isDirectConnection() {
        assertFalse(new ContextSupport(options).isDirectConnection());

        options.put(Options.CLIENT_CONNECTION_TYPE, "direct");
        assertTrue(new ContextSupport(options).isDirectConnection());

        options.put(Options.CLIENT_CONNECTION_TYPE, "gateway");
        assertFalse(new ContextSupport(options).isDirectConnection());
    }

    @Test
    void createManyConnectionsWithOkHttpConfigurator() throws Exception {
        options.put(Options.CLIENT_URI, makeClientUri());
        options.put("spark.marklogic.client.callTimeout", "10");
        options.put("spark.marklogic.client.connectionTimeout", "10");
        options.put("spark.marklogic.client.readTimeout", "10");
        options.put("spark.marklogic.client.writeTimeout", "10");

        final int clientsToCreate = 100;

        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < clientsToCreate; i++) {
            futures.add(service.submit(() -> new ContextSupport(options).connectToMarkLogic().release()));
        }

        int clientsCreated = 0;
        for (Future f : futures) {
            f.get();
            clientsCreated++;
        }
        service.shutdown();

        assertEquals(clientsToCreate, clientsCreated, "The expectation is that many clients can be created by " +
            "multiple threads at the same time due to the use of synchronization in ContextSupport. This won't be " +
            "necessary once we upgrade to Java Clien 6.5.1 or higher.");
    }
}
