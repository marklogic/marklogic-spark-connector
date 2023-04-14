package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Captures the results of analyzing a plan, which consists of the user's original plan parameterized with lower
 * and upper bound parameters, along with the calculated partitions.
 * <p>
 * This class and its inner classes must all be Serializable so that Spark can send them to different nodes in a
 * Spark cluster.
 */
class PlanAnalysis implements Serializable {

    final static long serialVersionUID = 1;

    final JsonNode boundedPlan;
    final List<Partition> partitions;

    PlanAnalysis(JsonNode boundedPlan, List<Partition> partitions) {
        this.boundedPlan = boundedPlan;
        this.partitions = partitions;
    }

    List<Bucket> getAllBuckets() {
        List<PlanAnalysis.Bucket> allBuckets = new ArrayList<>();
        partitions.forEach(partition -> allBuckets.addAll(partition.buckets));
        return allBuckets;
    }

    /**
     * Each partition corresponds to a Spark partition reader, where the reader is expected to make a call to MarkLogic
     * for each bucket in the partition.
     */
    static class Partition implements Serializable {

        final static long serialVersionUID = 1;

        final List<Bucket> buckets;

        Partition(long lowerBound, long upperBound, long bucketCount, long partitionSize) {
            long nextLowerBound = lowerBound;
            long bucketSize = Long.divideUnsigned(partitionSize, bucketCount);
            this.buckets = new ArrayList<>();
            for (int i = 1; i <= bucketCount; i++) {
                String lowerBoundStr = Long.toUnsignedString(nextLowerBound);
                String upperBoundStr = (i == bucketCount) ?
                Long.toUnsignedString(upperBound) :
                Long.toUnsignedString(nextLowerBound + bucketSize);
                buckets.add(new Bucket(lowerBoundStr, upperBoundStr));
                nextLowerBound = nextLowerBound + bucketSize + 1;
            }
        }
    }

    /**
     * Each bucket will correspond to a single call to /v1/rows in MarkLogic, with the lower and upper bounds being
     * applied against bounded plan.
     */
    static class Bucket implements Serializable {

        final static long serialVersionUID = 1;
        
        final String lowerBound;
        final String upperBound;

        Bucket(String lowerBound, String upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        // Only intended to help with debug logging
        public String toString() {
            return lowerBound + ":" + upperBound;
        }
    }
}
