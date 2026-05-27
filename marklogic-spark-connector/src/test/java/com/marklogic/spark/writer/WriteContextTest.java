/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.api.WriteListener;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WriteContextTest {

    @Test
    void commitListenerNotImplementingConsumerInterface() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_LISTENER_CLASS_NAME, NotAConsumer.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> writeContext.makeWriteListener());
        assertTrue(ex.getMessage().contains("does not implement WriteListener"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerWithNoMapConstructor() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_LISTENER_CLASS_NAME, NoMapConstructor.class.getName());

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> writeContext.makeWriteListener());
        assertTrue(ex.getMessage().contains("Failed to instantiate WriteListener class"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerNotFound() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_LISTENER_CLASS_NAME, "com.example.NonExistentClass");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
        ConnectorException ex = assertThrows(ConnectorException.class, () -> writeContext.makeWriteListener());
        assertTrue(ex.getMessage().contains("Failed to instantiate WriteListener class"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void commitListenerNotSpecified() {
        Map<String, String> props = new HashMap<>();
        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
        assertNull(writeContext.makeWriteListener());
    }

    @Test
    void commitListenerWithEmptyString() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_LISTENER_CLASS_NAME, "   ");

        WriteContext writeContext = new WriteContext(new StructType().add("test", DataTypes.StringType), props);
        assertNull(writeContext.makeWriteListener());
    }

    @Test
    void commitListenerWithParams() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.WRITE_LISTENER_CLASS_NAME, CommitResultsTestConsumer.class.getName());
        props.put(Options.WRITE_LISTENER_PARAM_PREFIX + "param1", "value1");
        props.put(Options.WRITE_LISTENER_PARAM_PREFIX + "param2", "value2");

        try {
            new WriteContext(new StructType().add("test", DataTypes.StringType), props).makeWriteListener();
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

    public static class NoMapConstructor implements WriteListener {
        public NoMapConstructor() {
        }

        @Override
        public void onWriteCommit(CommitResults commitResults) {
        }
    }
}
