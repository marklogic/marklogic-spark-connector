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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class ContextSupport implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(ContextSupport.class);
    private final Map<String, String> properties;

    protected ContextSupport(Map<String, String> properties) {
        this.properties = properties;
    }

    public DatabaseClient connectToMarkLogic() {
        Map<String, String> connectionProps = buildConnectionProperties();
        DatabaseClient client;
        try {
            client = DatabaseClientFactory.newClient(propertyName -> connectionProps.get("spark." + propertyName));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to connect to MarkLogic; cause: %s", e.getMessage()), e);
        }
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            throw new ConnectorException(String.format("Unable to connect to MarkLogic; status code: %d; error message: %s", result.getStatusCode(), result.getErrorMessage()));
        }
        return client;
    }

    protected final Map<String, String> buildConnectionProperties() {
        Map<String, String> connectionProps = new HashMap<>();
        connectionProps.put("spark.marklogic.client.authType", "digest");
        connectionProps.put("spark.marklogic.client.connectionType", "gateway");
        connectionProps.putAll(this.properties);

        if (optionExists(Options.CLIENT_URI)) {
            parseClientUri(properties.get(Options.CLIENT_URI), connectionProps);
        }

        if ("true".equalsIgnoreCase(properties.get(Options.CLIENT_SSL_ENABLED))) {
            connectionProps.put("spark.marklogic.client.sslProtocol", "default");
        }

        return connectionProps;
    }

    public final boolean optionExists(String option) {
        String value = properties.get(option);
        return value != null && value.trim().length() > 0;
    }

    private void parseClientUri(String clientUri, Map<String, String> connectionProps) {
        final String errorMessage = String.format("Invalid value for %s; must be username:password@host:port", Options.CLIENT_URI);
        String[] parts = clientUri.split("@");
        if (parts.length != 2) {
            throw new IllegalArgumentException(errorMessage);
        }
        String[] tokens = parts[0].split(":");
        if (tokens.length != 2) {
            throw new IllegalArgumentException(errorMessage);
        }
        connectionProps.put(Options.CLIENT_USERNAME, decodeValue(tokens[0], "username"));
        connectionProps.put(Options.CLIENT_PASSWORD, decodeValue(tokens[1], "password"));

        tokens = parts[1].split(":");
        if (tokens.length != 2) {
            throw new IllegalArgumentException(errorMessage);
        }
        connectionProps.put(Options.CLIENT_HOST, tokens[0]);
        if (tokens[1].contains("/")) {
            tokens = tokens[1].split("/");
            connectionProps.put(Options.CLIENT_PORT, tokens[0]);
            connectionProps.put(Options.CLIENT_DATABASE, tokens[1]);
        } else {
            connectionProps.put(Options.CLIENT_PORT, tokens[1]);
        }
    }

    private String decodeValue(String value, String label) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ConnectorException(String.format("Unable to decode %s; cause: %s", label, value));
        }
    }

    protected long getNumericOption(String optionName, long defaultValue, long minimumValue) {
        try {
            long value = this.getProperties().containsKey(optionName) ?
                Long.parseLong(this.getProperties().get(optionName)) :
                defaultValue;
            if (value < minimumValue) {
                throw new IllegalArgumentException(String.format("Value of '%s' option must be %d or greater", optionName, minimumValue));
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(String.format("Value of '%s' option must be numeric", optionName), ex);
        }
    }

    /**
     * Only intended for "write" use cases; an error on "read" is always expected to be propagated to the user.
     *
     * @return
     */
    public final boolean isAbortOnFailure() {
        return !"false".equalsIgnoreCase(getProperties().get(Options.WRITE_ABORT_ON_FAILURE));
    }

    public final boolean hasOption(String... options) {
        return Util.hasOption(this.properties, options);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
