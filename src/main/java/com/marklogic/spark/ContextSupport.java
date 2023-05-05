package com.marklogic.spark;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public abstract class ContextSupport implements Serializable {

    protected final static Logger logger = LoggerFactory.getLogger(ContextSupport.class);
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

    public Map<String, String> getProperties() {
        return properties;
    }
}
