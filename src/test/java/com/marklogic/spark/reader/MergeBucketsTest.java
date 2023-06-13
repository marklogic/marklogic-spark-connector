package com.marklogic.spark.reader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergeBucketsTest {

    @Test
    void threeBuckets() {
        PlanAnalysis.Partition p = new PlanAnalysis.Partition(1, 0, 1500, 3, 1500);

        assertEquals(3, p.buckets.size());
        assertEquals("0", p.buckets.get(0).lowerBound);
        assertEquals("500", p.buckets.get(0).upperBound);
        assertEquals("501", p.buckets.get(1).lowerBound);
        assertEquals("1001", p.buckets.get(1).upperBound);
        assertEquals("1002", p.buckets.get(2).lowerBound);
        assertEquals("1500", p.buckets.get(2).upperBound);

        PlanAnalysis.Partition p2 = p.mergeBuckets();

        assertEquals(1, p2.buckets.size());
        assertEquals("0", p2.buckets.get(0).lowerBound);
        assertEquals("1500", p2.buckets.get(0).upperBound);
    }

    @Test
    void oneBucket() {
        PlanAnalysis.Partition p = new PlanAnalysis.Partition(1, 0, 1000, 1, 1000);

        assertEquals(1, p.buckets.size());
        assertEquals("0", p.buckets.get(0).lowerBound);
        assertEquals("1000", p.buckets.get(0).upperBound);

        PlanAnalysis.Partition p2 = p.mergeBuckets();

        assertEquals(1, p2.buckets.size());
        assertEquals("0", p2.buckets.get(0).lowerBound);
        assertEquals("1000", p2.buckets.get(0).upperBound);
    }
}
