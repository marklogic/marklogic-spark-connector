package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.MarkLogicIOException;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

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
}
