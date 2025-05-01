/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MergeBucketsTest {

    @Test
    void threeBuckets() {
        PlanAnalysis.Partition p = new PlanAnalysis.Partition(1, 0, 1500, 3, 1500);

        assertEquals(3, p.getBuckets().size());
        assertEquals("0", p.getBuckets().get(0).lowerBound);
        assertEquals("500", p.getBuckets().get(0).upperBound);
        assertEquals("501", p.getBuckets().get(1).lowerBound);
        assertEquals("1001", p.getBuckets().get(1).upperBound);
        assertEquals("1002", p.getBuckets().get(2).lowerBound);
        assertEquals("1500", p.getBuckets().get(2).upperBound);

        PlanAnalysis.Partition p2 = p.mergeBuckets();

        assertEquals(1, p2.getBuckets().size());
        assertEquals("0", p2.getBuckets().get(0).lowerBound);
        assertEquals("1500", p2.getBuckets().get(0).upperBound);
    }

    @Test
    void oneBucket() {
        PlanAnalysis.Partition p = new PlanAnalysis.Partition(1, 0, 1000, 1, 1000);

        assertEquals(1, p.getBuckets().size());
        assertEquals("0", p.getBuckets().get(0).lowerBound);
        assertEquals("1000", p.getBuckets().get(0).upperBound);

        PlanAnalysis.Partition p2 = p.mergeBuckets();

        assertEquals(1, p2.getBuckets().size());
        assertEquals("0", p2.getBuckets().get(0).lowerBound);
        assertEquals("1000", p2.getBuckets().get(0).upperBound);
    }
}
