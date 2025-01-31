/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

public abstract class Context implements Serializable {

    private final Map<String, String> properties;

    protected Context(Map<String, String> properties) {
        this.properties = properties;
    }

    public final boolean hasOption(String... options) {
        return Stream.of(options)
            .anyMatch(option -> properties.get(option) != null && properties.get(option).trim().length() > 0);
    }

    public final String getStringOption(String option) {
        return getStringOption(option, null);
    }

    public final String getStringOption(String option, String defaultValue) {
        return hasOption(option) ? properties.get(option).trim() : defaultValue;
    }

    public final int getIntOption(String optionName, int defaultValue, int minimumValue) {
        return (int) getNumericOption(optionName, defaultValue, minimumValue);
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

    public final boolean getBooleanOption(String option, boolean defaultValue) {
        return hasOption(option) ? Boolean.parseBoolean(getStringOption(option)) : defaultValue;
    }

    public final String getOptionNameForMessage(String option) {
        return Util.getOptionNameForErrorMessage(option);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
