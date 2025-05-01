/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.SerializeUtil;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SerializeOpticReaderObjectsTest extends AbstractIntegrationTest {

    @Test
    void planAnalysis() {
        PlanAnalysis.Bucket bucket = new PlanAnalysis.Bucket("1", "2");
        PlanAnalysis.Partition partition = new PlanAnalysis.Partition("id", bucket);
        ObjectNode plan = objectMapper.createObjectNode();
        plan.put("hello", "world");
        PlanAnalysis planAnalysis = new PlanAnalysis(plan, Arrays.asList(partition), 0);

        planAnalysis = (PlanAnalysis) SerializeUtil.serialize(planAnalysis);
        assertEquals("world", planAnalysis.getSerializedPlan().get("hello").asText());
        bucket = planAnalysis.getPartitions().get(0).getBuckets().get(0);
        assertEquals("1", bucket.lowerBound);
        assertEquals("2", bucket.upperBound);
    }

    @Test
    void factory() {
        Map<String, String> props = new HashMap<>();
        props.put(Options.CLIENT_URI, makeClientUri());
        props.put(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY);
        OpticReadContext context = new OpticReadContext(props, new StructType().add("myType", DataTypes.StringType), 1);
        OpticPartitionReaderFactory factory = new OpticPartitionReaderFactory(context);

        factory = (OpticPartitionReaderFactory) SerializeUtil.serialize(factory);
        PlanAnalysis.Bucket bucket = new PlanAnalysis.Bucket("0", "1");
        PlanAnalysis.Partition partition = new PlanAnalysis.Partition("bucket-id", bucket);
        OpticPartitionReader reader = (OpticPartitionReader) factory.createReader(partition);
        assertNotNull(reader, "Creating the reader without error means the factory serialized correctly.");
    }
}
