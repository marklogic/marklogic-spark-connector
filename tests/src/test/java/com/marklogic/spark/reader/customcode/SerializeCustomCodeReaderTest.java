/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.SerializeUtil;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SerializeCustomCodeReaderTest extends AbstractIntegrationTest {

    @Test
    void test() {
        Map<String, String> options = new HashMap<>();
        options.put(Options.READ_JAVASCRIPT, "console.log()");
        options.put(Options.CLIENT_URI, makeClientUri());
        CustomCodeContext context = new CustomCodeContext(options, new StructType().add("myType", DataTypes.StringType));
        CustomCodePartitionReaderFactory factory = new CustomCodePartitionReaderFactory(context);

        factory = (CustomCodePartitionReaderFactory) SerializeUtil.serialize(factory);
        CustomCodePartition partition = new CustomCodePartition("p1");
        CustomCodePartitionReader reader = (CustomCodePartitionReader) factory.createReader(partition);
        assertNotNull(reader, "Creating the reader without error means the factory serialized correctly.");
    }

}
