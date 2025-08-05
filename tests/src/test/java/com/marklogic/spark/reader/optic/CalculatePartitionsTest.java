/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalculatePartitionsTest {

    @ParameterizedTest
    @CsvSource({
        "1,0,1,1",
        "2,0,2,2",
        "1,5000,1,2",
        "1,5001,1,2",
        "1,6666,1,2",
        "1,6667,1,2",
        "1,9999,1,2",
        "1,10000,1,1",
        "1,10001,1,1",
        "3,3000,3,6"
    })
    void test(long userPartitionCount, long batchSize, int expectedPartitionCount, int expectedBucketCount) {
        long rowCount = 10000;
        List<PlanAnalysis.Partition> partitions = PlanAnalyzer.calculatePartitions(rowCount, userPartitionCount, batchSize);
        int bucketCount = 0;
        for (PlanAnalysis.Partition partition : partitions) {
            bucketCount += partition.getBuckets().size();
        }

        assertEquals(expectedPartitionCount, partitions.size(), "Unexpected number of partitions");
        assertEquals(expectedBucketCount, bucketCount, "Unexpected number of buckets");
    }
}
