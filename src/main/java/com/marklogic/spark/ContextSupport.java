/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.extra.okhttpclient.OkHttpClientConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ContextSupport implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(ContextSupport.class);
    private final Map<String, String> properties;
    private final boolean configuratorWasAdded;

    // Java Client 6.5.0 has a bug in it (to be fixed in 6.5.1 or 6.6.0) where multiple threads that use a configurator
    // can run into a ConcurrentModificationException. So need to synchronize adding a configurator and creating a
    // client. Those two actions are rarely done, so the cost of synchronization will be negligible.
    private static final Object CLIENT_LOCK = new Object();

    public ContextSupport(Map<String, String> properties) {
        this.properties = properties;
        this.configuratorWasAdded = addOkHttpConfiguratorIfNecessary();
    }

    public DatabaseClient connectToMarkLogic() {
        return connectToMarkLogic(null);
    }

    /**
     * @param host if not null, overrides the user-defined host. Used for direct connections when a load balancer is
     *             not in front of MarkLogic.
     * @return
     */
    public DatabaseClient connectToMarkLogic(String host) {
        Map<String, String> connectionProps = buildConnectionProperties();
        if (host != null) {
            connectionProps.put(Options.CLIENT_HOST, host);
        }
        DatabaseClient client;
        if (configuratorWasAdded) {
            synchronized (CLIENT_LOCK) {
                client = connect(connectionProps);
            }
        } else {
            client = connect(connectionProps);
        }
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            if (result.getStatusCode() == 404) {
                throw new ConnectorException(String.format("Unable to connect to MarkLogic; status code: 404; ensure that " +
                    "you are attempting to connect to a MarkLogic REST API app server. See the MarkLogic documentation on " +
                    "REST API app servers for more information."));
            }
            throw new ConnectorException(String.format(
                "Unable to connect to MarkLogic; status code: %d; error message: %s", result.getStatusCode(), result.getErrorMessage()));
        }
        return client;
    }

    private DatabaseClient connect(Map<String, String> connectionProps) {
        try {
            return DatabaseClientFactory.newClient(propertyName -> connectionProps.get("spark." + propertyName));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to connect to MarkLogic; cause: %s", e.getMessage()), e);
        }
    }

    protected final Map<String, String> buildConnectionProperties() {
        Map<String, String> connectionProps = new HashMap<>();
        connectionProps.put("spark.marklogic.client.authType", "digest");
        connectionProps.put("spark.marklogic.client.connectionType", "gateway");
        connectionProps.putAll(this.properties);
        if (optionExists(Options.CLIENT_URI)) {
            parseConnectionString(properties.get(Options.CLIENT_URI), connectionProps);
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

    public final String getOptionNameForMessage(String option) {
        return Util.getOptionNameForErrorMessage(option);
    }

    private void parseConnectionString(String value, Map<String, String> connectionProps) {
        ConnectionString connectionString = new ConnectionString(value, getOptionNameForMessage("spark.marklogic.client.uri"));
        connectionProps.put(Options.CLIENT_USERNAME, connectionString.getUsername());
        connectionProps.put(Options.CLIENT_PASSWORD, connectionString.getPassword());
        connectionProps.put(Options.CLIENT_HOST, connectionString.getHost());
        connectionProps.put(Options.CLIENT_PORT, connectionString.getPort() + "");

        String db = connectionString.getDatabase();
        if (db != null && db.trim().length() > 0) {
            connectionProps.put(Options.CLIENT_DATABASE, db);
        }
    }

    public final long getNumericOption(String optionName, long defaultValue, long minimumValue) {
        try {
            long value = this.getProperties().containsKey(optionName) ?
                Long.parseLong(this.getProperties().get(optionName)) :
                defaultValue;
            if (value != defaultValue && value < minimumValue) {
                throw new ConnectorException(String.format("The value of '%s' must be %d or greater.", getOptionNameForMessage(optionName), minimumValue));
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new ConnectorException(String.format("The value of '%s' must be numeric.", getOptionNameForMessage(optionName)), ex);
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

    public final boolean isDirectConnection() {
        String value = getProperties().get(Options.CLIENT_CONNECTION_TYPE);
        return value != null && value.equalsIgnoreCase(DatabaseClient.ConnectionType.DIRECT.name());
    }

    public final boolean hasOption(String... options) {
        return Util.hasOption(this.properties, options);
    }

    public final String getStringOption(String option) {
        return getStringOption(option, null);
    }

    public final String getStringOption(String option, String defaultValue) {
        return hasOption(option) ? properties.get(option).trim() : defaultValue;
    }

    public final boolean getBooleanOption(String option, boolean defaultValue) {
        return hasOption(option) ? Boolean.parseBoolean(getStringOption(option)) : defaultValue;
    }

    public final boolean isStreamingFiles() {
        return "true".equalsIgnoreCase(getStringOption(Options.STREAM_FILES));
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * @return true if a configurator was added
     */
    private boolean addOkHttpConfiguratorIfNecessary() {
        final String prefix = "spark.marklogic.client.";
        final long defaultValue = -1;
        final long connectionTimeout = getNumericOption(prefix + "connectionTimeout", defaultValue, defaultValue);
        final long callTimeout = getNumericOption(prefix + "callTimeout", defaultValue, defaultValue);
        final long readTimeout = getNumericOption(prefix + "connectionTimeout", defaultValue, defaultValue);
        final long writeTimeout = getNumericOption(prefix + "writeTimeout", defaultValue, defaultValue);

        if (connectionTimeout > -1 || callTimeout > -1 || readTimeout > -1 || writeTimeout > -1) {
            synchronized (CLIENT_LOCK) {
                DatabaseClientFactory.addConfigurator((OkHttpClientConfigurator) builder -> {
                    if (connectionTimeout > -1) {
                        builder.connectTimeout(connectionTimeout, TimeUnit.SECONDS);
                    }
                    if (callTimeout > -1) {
                        builder.callTimeout(callTimeout, TimeUnit.SECONDS);
                    }
                    if (readTimeout > -1) {
                        builder.readTimeout(readTimeout, TimeUnit.SECONDS);
                    }
                    if (writeTimeout > -1) {
                        builder.writeTimeout(writeTimeout, TimeUnit.SECONDS);
                    }
                });
            }
            return true;
        }
        return false;
    }
}
