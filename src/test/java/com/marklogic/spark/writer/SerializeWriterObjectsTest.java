/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.SerializeUtil;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SerializeWriterObjectsTest extends AbstractIntegrationTest {

    @Test
    void test() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.CLIENT_URI, makeClientUri());
        WriteContext writeContext = new WriteContext(new StructType().add("myType", DataTypes.StringType), props);
        WriteBatcherDataWriterFactory factory = new WriteBatcherDataWriterFactory(writeContext, null);

        factory = (WriteBatcherDataWriterFactory) SerializeUtil.serialize(factory);
        WriteBatcherDataWriter writer = (WriteBatcherDataWriter) factory.createWriter(1, 1l);
        assertNotNull(writer, "Creating the writer without error means the factory serialized correctly.");
    }
}
