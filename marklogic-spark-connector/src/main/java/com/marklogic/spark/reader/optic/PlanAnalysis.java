/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.Util;
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

    private final ObjectNode serializedPlan;
    private final List<Partition> partitions;
    private final long serverTimestamp;

    PlanAnalysis(ObjectNode serializedPlan, List<Partition> partitions, long serverTimestamp) {
        this.serializedPlan = serializedPlan;
        this.partitions = partitions;
        this.serverTimestamp = serverTimestamp;
    }


    void pushOperatorIntoPlan(ObjectNode operator) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Adding operator to plan: {}", operator);
        }
        ArrayNode operators = (ArrayNode) serializedPlan.get("$optic").get("args");
        int index = isLastOperatorAPrepareCall(operators) ? operators.size() - 1 : operators.size();
        operators.insert(index, operator);
    }

    /**
     * The internal/viewinfo endpoint is known to add an op:prepare operator at the end of the list of operator
     * args. Each operator added by the connector based on pushdowns needs to be added before this op:prepare
     * operator; otherwise, MarkLogic will throw an error.
     * <p>
     * The op:prepare operator won't be present though for a non-fromView query, as internal/viewinfo won't be invoked
     * for such a query.
     *
     * @param operators
     */
    private boolean isLastOperatorAPrepareCall(ArrayNode operators) {
        JsonNode lastOperator = operators.get(operators.size() - 1);
        return lastOperator.has("fn") && "prepare".equals(lastOperator.get("fn").asText());
    }

    long getServerTimestamp() {
        return serverTimestamp;
    }

    ObjectNode getSerializedPlan() {
        return serializedPlan;
    }

    List<Partition> getPartitions() {
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

        static Partition singleCallPartition() {
            return new Partition("[number: 1]", new Bucket("0", "0"));
        }

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

        boolean isSingleCallToMarkLogic() {
            return "0".equals(lowerBound) && "0".equals(upperBound);
        }

        // Only intended to help with debug logging
        public String toString() {
            return String.format("[%s:%s]", lowerBound, upperBound);
        }
    }
}
