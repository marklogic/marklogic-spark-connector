package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import java.io.Serializable;
import java.util.Map;

public abstract class ContextSupport implements Serializable {

    private final Map<String, String> properties;

    protected ContextSupport(Map<String, String> properties) {
        this.properties = properties;
    }

    public DatabaseClient connectToMarkLogic() {
        DatabaseClient client = DatabaseClientFactory.newClient(propertyName -> properties.get("spark." + propertyName));
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            throw new RuntimeException(String.format("Unable to connect to MarkLogic; status code: %d; error message: %s", result.getStatusCode(), result.getErrorMessage()));
        }
        return client;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
