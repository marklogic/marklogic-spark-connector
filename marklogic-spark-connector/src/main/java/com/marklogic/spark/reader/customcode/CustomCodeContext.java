/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.spark.ContextSupport;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class CustomCodeContext extends ContextSupport {

    private final StructType schema;
    private final boolean customSchema;

    public CustomCodeContext(Map<String, String> properties, StructType schema) {
        super(properties);
        this.schema = schema;
        boolean isDefaultSchema = schema.fields().length == 1 &&
            DataTypes.StringType.equals(schema.fields()[0].dataType());
        this.customSchema = !isDefaultSchema;
    }

    public StructType getSchema() {
        return schema;
    }

    public boolean isCustomSchema() {
        return customSchema;
    }
}
