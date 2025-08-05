/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.SerializeUtil;
import com.marklogic.spark.reader.customcode.CustomCodeContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SerializeCustomCodeWriterTest extends AbstractIntegrationTest {

    @Test
    void test() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.CLIENT_URI, makeClientUri());
        CustomCodeContext context = new CustomCodeContext(options, new StructType().add("myType", DataTypes.StringType), "prefix");
        CustomCodeWriterFactory factory = new CustomCodeWriterFactory(context);

        factory = (CustomCodeWriterFactory) SerializeUtil.serialize(factory);
        CustomCodeWriter writer = (CustomCodeWriter) factory.createWriter(1, 1l);
        assertNotNull(writer, "Creating the writer without error means the factory serialized correctly.");
    }
}
