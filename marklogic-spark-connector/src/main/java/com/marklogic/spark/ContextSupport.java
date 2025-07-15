/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.extra.okhttpclient.OkHttpClientConfigurator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ContextSupport extends Context implements Serializable {

    private final boolean configuratorWasAdded;

    // Java Client 6.5.0 has a bug in it (to be fixed in 6.5.1 or 6.6.0) where multiple threads that use a configurator
    // can run into a ConcurrentModificationException. So need to synchronize adding a configurator and creating a
    // client. Those two actions are rarely done, so the cost of synchronization will be negligible.
    private static final Object CLIENT_LOCK = new Object();

    public ContextSupport(Map<String, String> properties) {
        super(properties);
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
        connectionProps.putAll(getProperties());
        if (connectionProps.containsKey(Options.CLIENT_URI)) {
            connectionProps.put(Options.CLIENT_CONNECTION_STRING, connectionProps.get(Options.CLIENT_URI));
        }
        if ("true".equalsIgnoreCase(getProperties().get(Options.CLIENT_SSL_ENABLED))) {
            connectionProps.put("spark.marklogic.client.sslProtocol", "default");
        }
        return connectionProps;
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

    public final boolean isStreamingFiles() {
        return "true".equalsIgnoreCase(getStringOption(Options.STREAM_FILES));
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

    public static Set<DocumentManager.Metadata> getRequestedMetadata(ContextSupport context) {
        Set<DocumentManager.Metadata> set = new HashSet<>();
        if (context.hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            for (String category : context.getStringOption(Options.READ_DOCUMENTS_CATEGORIES).split(",")) {
                if ("content".equalsIgnoreCase(category)) {
                    continue;
                }
                if ("metadata".equalsIgnoreCase(category)) {
                    set.add(DocumentManager.Metadata.ALL);
                } else {
                    set.add(DocumentManager.Metadata.valueOf(category.toUpperCase()));
                }
            }
        }
        return set;
    }
}
