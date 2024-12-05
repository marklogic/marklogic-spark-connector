/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;

import java.util.ArrayList;
import java.util.List;

/**
 * "Analyze" = take a user's plan (from their Optic DSL query) and parameterize it with lower and upper bounds,
 * and also calculate partitions. Can only be used when the user has an "op.fromView" query, as the
 * internal/viewinfo endpoint can only partition that query.
 */
class PlanAnalyzer {

    private final DatabaseClientImpl databaseClient;
    private final RowManager rowManager;

    PlanAnalyzer(DatabaseClientImpl databaseClient) {
        this.databaseClient = databaseClient;
        this.rowManager = databaseClient.newRowManager();
    }

    PlanAnalysis analyzePlan(String dslQuery, long userPartitionCount, long userBatchSize) {
        RawQueryDSLPlan dslPlan = rowManager.newRawQueryDSLPlan(new StringHandle(dslQuery));

        JsonNode viewInfo = databaseClient.getServices().postResource(
            null, "internal/viewinfo", null, null, dslPlan.getHandle(), new JacksonHandle()
        ).get();

        long rowCount = viewInfo.get("rowCount").asLong(0);
        List<PlanAnalysis.Partition> partitions = calculatePartitions(rowCount, userPartitionCount, userBatchSize);

        // Establish a server timestamp so each call to get rows is at the same timestamp.
        long serverTimestamp = databaseClient.newRowManager().columnInfo(dslPlan, new StringHandle()).getServerTimestamp();
        return new PlanAnalysis(dslQuery, (ObjectNode) viewInfo.get("modifiedPlan"), partitions, serverTimestamp);
    }

    static List<PlanAnalysis.Partition> calculatePartitions(long rowCount, long userPartitionCount, long userBatchSize) {
        final long batchSize = userBatchSize > 0 ? userBatchSize : Long.parseLong("-1");

        long bucketsPerPartition = calculateBucketsPerPartition(rowCount, userPartitionCount, batchSize);
        long partitionSize = Long.divideUnsigned(-1, userPartitionCount);
        long nextLowerBound = 0;

        List<PlanAnalysis.Partition> partitions = new ArrayList<>();
        for (int i = 1; i <= userPartitionCount; i++) {
            long upperBound = (i == userPartitionCount) ? -1 : nextLowerBound + partitionSize;
            partitions.add(new PlanAnalysis.Partition(i, nextLowerBound, upperBound, bucketsPerPartition, partitionSize));
            nextLowerBound = nextLowerBound + partitionSize + 1;
        }
        return partitions;
    }

    /**
     * The number of buckets per partition is always the same, as the random distribution of row IDs means we don't know
     * how rows will be distributed across buckets.
     */
    private static long calculateBucketsPerPartition(long rowCount, long userPartitionCount, long batchSize) {
        double rawBucketsPerPartition = ((double) rowCount / userPartitionCount) / batchSize;
        // ceil is used here to ensure that given the batch size, a bucket typically will not have more rows in it
        // than the batch size. That's not guaranteed, as row IDs could have a distribution such that many rows are in
        // one particular bucket.
        long bucketsPerPartition = (long) Math.ceil(rawBucketsPerPartition);
        return bucketsPerPartition < 1 ? 1 : bucketsPerPartition;
    }
}
