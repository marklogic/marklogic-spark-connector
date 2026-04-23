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
    void commitListenerNotImplementingConsumerInterface() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_LISTENER_CLASSNAME, NotAConsumer.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("does not implement Consumer interface"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerWithNoMapConstructor() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_LISTENER_CLASSNAME, NoMapConstructor.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("Unable to instantiate commit listener"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerNotFound() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_LISTENER_CLASSNAME, "com.example.NonExistentClass");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> new MarkLogicWrite(writeContext));
        assertTrue(ex.getMessage().contains("Unable to instantiate commit listener"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerNotSpecified() {
        Map<String, String> props = new HashMap<>();
        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        // Should not throw an exception when the option is not specified
        assertDoesNotThrow(() -> new MarkLogicWrite(writeContext));
    }

    @Test
    void commitListenerWithEmptyString() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_LISTENER_CLASSNAME, "   ");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);

        // Should not throw an exception when the option is empty/whitespace
        assertDoesNotThrow(() -> new MarkLogicWrite(writeContext));
    }

    @Test
    void commitListenerWithParams() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_COMMIT_LISTENER_CLASSNAME, CommitResultsTestConsumer.class.getName());
        props.put(Options.WRITE_COMMIT_LISTENER_PARAM_PREFIX + "param1", "value1");
        props.put(Options.WRITE_COMMIT_LISTENER_PARAM_PREFIX + "param2", "value2");

        try {
            WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
            new MarkLogicWrite(writeContext);
            Map<String, String> params = CommitResultsTestConsumer.params;
            assertEquals(2, params.size());
            assertEquals("value1", params.get("param1"));
            assertEquals("value2", params.get("param2"));
        } finally {
            CommitResultsTestConsumer.reset();
        }
    }

    // Test helper classes

    public static class NotAConsumer {
        public NotAConsumer(Map<String, String> params) {
        }
    }

    public static class NoMapConstructor implements Consumer<Map<String, Object>> {
        public NoMapConstructor() {
        }

        @Override
        public void accept(Map<String, Object> results) {
        }
    }
}
