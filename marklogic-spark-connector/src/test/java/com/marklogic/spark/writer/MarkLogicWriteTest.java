/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class MarkLogicWriteTest {

    @Test
    void commitResultsConsumerNotImplementingConsumerInterface() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME, NotAConsumer.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("does not implement Consumer interface"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitResultsConsumerWithNoZeroArgConstructor() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME, NoZeroArgConstructor.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("Unable to instantiate commit results consumer"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitResultsConsumerNotFound() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME, "com.example.NonExistentClass");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("Unable to instantiate commit results consumer"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitResultsConsumerNotSpecified() {
        Map<String, String> props = new HashMap<>();
        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        // Should not throw an exception when the option is not specified
        assertDoesNotThrow(() -> new MarkLogicWrite(writeContext));
    }

    @Test
    void commitResultsConsumerWithEmptyString() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME, "   ");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        // Should not throw an exception when the option is empty/whitespace
        assertDoesNotThrow(() -> new MarkLogicWrite(writeContext));
    }

    // Test helper classes

    public static class NotAConsumer {
        public NotAConsumer() {
        }
    }

    public static class NoZeroArgConstructor implements Consumer<Map<String, Object>> {
        public NoZeroArgConstructor(String requiredArg) {
        }

        @Override
        public void accept(Map<String, Object> results) {
        }
    }
}
