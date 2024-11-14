/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BuildConnectionPropertiesTest {

    private static final String AUTH_TYPE = Options.CLIENT_AUTH_TYPE;
    private static final String CONNECTION_TYPE = Options.CLIENT_CONNECTION_TYPE;

    private Map<String, String> properties = new HashMap<>();

    @Test
    void useDefaults() {
        Map<String, String> connectionProps = new ContextSupport(properties).buildConnectionProperties();
        assertEquals("digest", connectionProps.get(AUTH_TYPE));
        assertEquals("gateway", connectionProps.get(CONNECTION_TYPE),
            "To avoid an error when the user uses the connector to write data to a cluster behind a load balancer, " +
                "the connector defaults to a 'gateway' connection type. If the user can connect directly to MarkLogic, " +
                "they can simply set this to 'direct' instead.");
    }

    @Test
    void clientUriWithDatabase() {
        properties.put(Options.CLIENT_DATABASE, "Documents");
        properties.put(Options.CLIENT_URI, "user:password@host:8016");
        Map<String, String> connectionProps = new ContextSupport(properties).buildConnectionProperties();

        assertEquals("Documents", connectionProps.get(Options.CLIENT_DATABASE), "If the user does not specify a " +
            "database in the connection string, then the database value specified separately should still be used. " +
            "This avoids a potential issue where the user is not aware that the connection string accepts a database " +
            "and thus specifies it via the database option.");
        assertEquals("user", connectionProps.get(Options.CLIENT_USERNAME));
        assertEquals("password", connectionProps.get(Options.CLIENT_PASSWORD));
        assertEquals("host", connectionProps.get(Options.CLIENT_HOST));
        assertEquals("8016", connectionProps.get(Options.CLIENT_PORT));
    }

    @Test
    void overrideDefaults() {
        properties.put(AUTH_TYPE, "basic");
        properties.put(CONNECTION_TYPE, "direct");
        Map<String, String> connectionProps = new ContextSupport(properties).buildConnectionProperties();
        assertEquals("basic", connectionProps.get(AUTH_TYPE));
        assertEquals("direct", connectionProps.get(CONNECTION_TYPE));
    }

    @Test
    void sslEnabled() {
        properties.put(Options.CLIENT_SSL_ENABLED, "true");

        Map<String, String> connectionProps = new ContextSupport(properties).buildConnectionProperties();
        assertEquals("default", connectionProps.get("spark.marklogic.client.sslProtocol"),
            "While the Java Client allows for actual protocol values for the sslProtocol property, the SSLContext " +
                "that's created still needs an X509TrustManager. But since Spark options only allow for simple values, " +
                "there's no way for a Spark user to provide a custom X509TrustManager. It appears the only possibility " +
                "is to use the JVM's default trust manager. Thus, instead of forcing the user to set " +
                "sslProtocol=default (which implies there are other valid choices), the Spark connector lets a user " +
                "set sslEnabled=true, which is a shortcut for requesting that the JVM's default trust manager be used.");
    }

    @Test
    void sslDisabled() {
        properties.put(Options.CLIENT_SSL_ENABLED, "false");

        Map<String, String> connectionProps = new ContextSupport(properties).buildConnectionProperties();
        assertFalse(connectionProps.containsKey("spark.marklogic.client.sslProtocol"));
    }
}
