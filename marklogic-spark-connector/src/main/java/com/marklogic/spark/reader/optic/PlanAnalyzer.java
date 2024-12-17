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
import com.marklogic.spark.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Produces one or more partitions for breaking a user's Optic DSL query up - unless it does not use "op.fromView", in
 * which case a single call will be made to MarkLogic for the query. This is due to the "internal/view-info" endpoint
 * only supporting partitioning on "op.fromView" queries as of MarkLogic 12 EA1.
 */
class PlanAnalyzer {

    // Used to convert a non-fromView DSL query into a serialized JSON plan.
    // Uses an xdmp.invoke; ran into issues with passing the DSL query in as a variable when using 'import' on the
    // optic-dsl-js.mjs module. See MLE-18460 for more information.
    private static final String PLAN_EXPORT_QUERY = "var dslQuery; xdmp.invoke('/MarkLogic/optic/optic-dsl-js-export.mjs', {query:dslQuery})";

    private final DatabaseClientImpl databaseClient;
    private final RowManager rowManager;

    PlanAnalyzer(DatabaseClientImpl databaseClient) {
        this.databaseClient = databaseClient;
        this.rowManager = databaseClient.newRowManager();
    }

    PlanAnalysis analyzePlan(String dslQuery, long userPartitionCount, long userBatchSize) {
        final boolean queryCanBePartitioned = dslQuery.contains("op.fromView");
        return queryCanBePartitioned ?
            readRowsViaMultipleCallsToMarkLogic(dslQuery, userPartitionCount, userBatchSize) :
            readRowsInSingleCallToMarkLogic(dslQuery);
    }

    private PlanAnalysis readRowsViaMultipleCallsToMarkLogic(String dslQuery, long userPartitionCount, long userBatchSize) {
        RawQueryDSLPlan dslPlan = rowManager.newRawQueryDSLPlan(new StringHandle(dslQuery));

        JsonNode viewInfo = databaseClient.getServices().postResource(
            null, "internal/viewinfo", null, null, dslPlan.getHandle(), new JacksonHandle()
        ).get();

        long rowCount = viewInfo.get("rowCount").asLong(0);
        List<PlanAnalysis.Partition> partitions = calculatePartitions(rowCount, userPartitionCount, userBatchSize);

        // Establish a server timestamp so each call to get rows is at the same timestamp.
        long serverTimestamp = databaseClient.newRowManager().columnInfo(dslPlan, new StringHandle()).getServerTimestamp();
        return new PlanAnalysis((ObjectNode) viewInfo.get("modifiedPlan"), partitions, serverTimestamp);
    }

    private PlanAnalysis readRowsInSingleCallToMarkLogic(String dslQuery) {
        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Optic query does not contain 'op.fromView', so will read rows in a single call to MarkLogic.");
        }

        ObjectNode plan = (ObjectNode) databaseClient.newServerEval()
            .javascript(PLAN_EXPORT_QUERY)
            .addVariable("dslQuery", dslQuery)
            .evalAs(JsonNode.class);

        return new PlanAnalysis(plan, Arrays.asList(PlanAnalysis.Partition.singleCallPartition()), 0);
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
