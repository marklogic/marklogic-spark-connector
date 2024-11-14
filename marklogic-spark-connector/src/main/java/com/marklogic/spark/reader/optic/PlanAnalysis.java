/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Captures the results of analyzing a plan, which consists of the user's original plan parameterized with lower
 * and upper bound parameters, along with the calculated partitions.
 * <p>
 * This class and its inner classes must all be Serializable so that Spark can send them to different nodes in a
 * Spark cluster.
 */
class PlanAnalysis implements Serializable {

    static final long serialVersionUID = 1;

    private final ObjectNode boundedPlan;
    private final List<Partition> partitions;

    PlanAnalysis(ObjectNode boundedPlan, List<Partition> partitions) {
        this.boundedPlan = boundedPlan;
        this.partitions = partitions;
    }

    public ObjectNode getBoundedPlan() {
        return boundedPlan;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    List<Bucket> getAllBuckets() {
        List<PlanAnalysis.Bucket> allBuckets = new ArrayList<>();
        partitions.forEach(partition -> allBuckets.addAll(partition.buckets));
        return allBuckets;
    }

    Partition[] getPartitionArray() {
        return this.partitions.toArray(new Partition[]{});
    }

    /**
     * Each partition corresponds to a Spark partition reader, where the reader is expected to make a call to MarkLogic
     * for each bucket in the partition.
     */
    static class Partition implements InputPartition, Serializable {

        static final long serialVersionUID = 1;

        private final String identifier; // used solely for logging purposes
        private final List<Bucket> buckets;

        Partition(int number, long lowerBound, long upperBound, long bucketCount, long partitionSize) {
            this.identifier = String.format("[number: %d; lower bound: %s; upper bound: %s]",
                number,
                Long.toUnsignedString(lowerBound),
                Long.toUnsignedString(upperBound)
            );

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

        Partition(String identifier, Bucket bucket) {
            this.identifier = identifier;
            this.buckets = bucket != null ? Arrays.asList(bucket) : new ArrayList<>();
        }

        /**
         * Similar to a copy constructor; used to construct a new Partition with a single bucket based on the
         * buckets in the given Partition.
         *
         * @return
         */
        Partition mergeBuckets() {
            if (buckets == null || buckets.isEmpty()) {
                return new Partition(identifier, null);
            }
            String lowerBound = buckets.get(0).lowerBound;
            String upperBound = buckets.get(buckets.size() - 1).upperBound;
            return new Partition(identifier, new Bucket(lowerBound, upperBound));
        }

        public String getIdentifier() {
            return identifier;
        }

        public List<Bucket> getBuckets() {
            return buckets;
        }

        @Override
        public String toString() {
            return this.identifier;
        }
    }

    /**
     * Each bucket will correspond to a single call to /v1/rows in MarkLogic, with the lower and upper bounds being
     * applied against bounded plan.
     */
    static class Bucket implements Serializable {

        static final long serialVersionUID = 1;

        final String lowerBound;
        final String upperBound;

        Bucket(String lowerBound, String upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        // Only intended to help with debug logging
        public String toString() {
            return String.format("[%s:%s]", lowerBound, upperBound);
        }
    }
}
