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
package com.marklogic.spark;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class BuildConnectionPropertiesTest {

    private final static String AUTH_TYPE = "spark.marklogic.client.authType";
    private final static String CONNECTION_TYPE = "spark.marklogic.client.connectionType";

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
