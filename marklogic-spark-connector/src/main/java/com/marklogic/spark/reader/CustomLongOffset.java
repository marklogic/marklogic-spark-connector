/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader;

import org.apache.spark.sql.connector.read.streaming.Offset;

// Added to avoid dependency on Spark's LongOffset class, which was removed in 4.1.0-preview2.
public class CustomLongOffset extends Offset {

    private final long value;

    public CustomLongOffset(long value) {
        this.value = value;
    }

    @Override
    public String json() {
        return String.valueOf(value);
    }

    public long getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
