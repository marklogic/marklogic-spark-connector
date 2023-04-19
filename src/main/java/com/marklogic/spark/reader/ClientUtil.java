package com.marklogic.spark.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import java.util.Map;

abstract class ClientUtil {

    static DatabaseClient connectToMarkLogic(Map<String, String> sparkProperties) {
        DatabaseClient client = DatabaseClientFactory.newClient(propertyName -> sparkProperties.get(propertyName));
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            throw new RuntimeException(String.format("Unable to connect to MarkLogic; status code: %d; error message: %s", result.getStatusCode(), result.getErrorMessage()));
        }
        return client;
    }
}
