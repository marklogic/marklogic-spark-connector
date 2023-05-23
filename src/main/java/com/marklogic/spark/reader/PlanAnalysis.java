/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader;

import com.fasterxml.jackson.databind.JsonNode;
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

    final static long serialVersionUID = 1;

    final JsonNode boundedPlan;
    final List<Partition> partitions;

    PlanAnalysis(JsonNode boundedPlan, List<Partition> partitions) {
        this.boundedPlan = boundedPlan;
        this.partitions = partitions;
    }

    /**
     * Copy constructor for creating a new plan analysis with the given plan and a single bucket. Used for pushing down
     * aggregate operations that can be efficiently calculated by MarkLogic in a single request.
     *
     * @param boundedPlan
     */
    PlanAnalysis(JsonNode boundedPlan) {
        this.boundedPlan = boundedPlan;
        final String maxUnsignedLong = "18446744073709551615";
        this.partitions = Arrays.asList(new Partition(0, new Bucket("0", maxUnsignedLong)));
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

        final static long serialVersionUID = 1;

        final String identifier; // used solely for logging purposes
        final List<Bucket> buckets;

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

        /**
         * For micro-batch reading, where each Spark task is intended to process a single bucket, and thus each
         * partition should contain a single bucket.
         *
         * @param bucketIndex
         * @param bucket
         */
        Partition(int bucketIndex, Bucket bucket) {
            this.identifier = bucketIndex + "";
            this.buckets = Arrays.asList(bucket);
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

        final static long serialVersionUID = 1;

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
