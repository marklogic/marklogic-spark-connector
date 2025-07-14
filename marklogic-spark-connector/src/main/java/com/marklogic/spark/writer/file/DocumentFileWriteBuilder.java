/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class DocumentFileWriteBuilder implements WriteBuilder {

    private final Map<String, String> properties;
    private final StructType schema;

    public DocumentFileWriteBuilder(Map<String, String> properties, StructType schema) {
        this.properties = properties;
        this.schema = schema;
    }

    @Override
    public Write build() {
        return new Write() {
            @Override
            public BatchWrite toBatch() {
                return new DocumentFileBatch(properties, schema);
            }
        };
    }
}
