/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AnalyzePlanTest extends AbstractIntegrationTest {

    private RowManager rowManager;

    @BeforeEach
    void setup() {
        rowManager = getDatabaseClient().newRowManager();
    }

    @ParameterizedTest
    @CsvSource({
        "1,0",
        "1,5",
        "1,15",
        "2,0",
        "2,5",
        "2,15",
        "5,0",
        "5,5",
        "5,15"
    })
    void partitionCountAndBatchSize(long partitionCount, long batchSize) {
        logger.info("{}:{}", partitionCount, batchSize);

        PlanAnalysis planAnalysis = analyzePlan(partitionCount, batchSize);
        verifyBucketsCoverAllUnsignedLongs(planAnalysis);
        verifyAllFifteenAuthorsAreReturned(planAnalysis);
    }

    private PlanAnalysis analyzePlan(long partitionCount, long batchSize) {
        PlanAnalyzer partitioner = new PlanAnalyzer((DatabaseClientImpl) getDatabaseClient());
        String dslQuery = "op.fromView('Medical', 'Authors').select(['LastName', 'rowID'])";
        PlanAnalysis planAnalysis = partitioner.analyzePlan(dslQuery, partitionCount, batchSize);
        assertEquals(partitionCount, planAnalysis.getPartitions().size());
        return planAnalysis;
    }

    /**
     * Verifies that the sequence of buckets across all the partitions covers all unsigned longs from 0 to max unsigned
     * long.
     *
     * @param planAnalysis
     */
    private void verifyBucketsCoverAllUnsignedLongs(PlanAnalysis planAnalysis) {
        List<PlanAnalysis.Bucket> allBuckets = planAnalysis.getAllBuckets();

        assertEquals("0", allBuckets.get(0).lowerBound, "The first bucket in the first partition should have a lower " +
            "bound of the lowest unsigned long, which is zero.");
        assertEquals("18446744073709551615", allBuckets.get(allBuckets.size() - 1).upperBound, "The last bucket in the " +
            "last partition should have the highest unsigned long as its upper bound.");

        for (int i = 1; i < allBuckets.size(); i++) {
            PlanAnalysis.Bucket bucket = allBuckets.get(i);
            PlanAnalysis.Bucket previousBucket = allBuckets.get(i - 1);
            assertEquals(Long.parseUnsignedLong(bucket.lowerBound), Long.parseUnsignedLong(previousBucket.upperBound) + 1,
                "The lower bound of each bucket should be 1 more than the upper bound of the previous bucket.");
        }
    }

    /**
     * Runs the plan for each bucket, ensuring that all 15 authors are returned.
     *
     * @param planAnalysis
     */
    private void verifyAllFifteenAuthorsAreReturned(PlanAnalysis planAnalysis) {
        // Run the first bucket plan to get the serverTimestamp
        JacksonHandle initialHandle = new JacksonHandle();
        runPlan(planAnalysis, planAnalysis.getPartitions().get(0).getBuckets().get(0), initialHandle);
        final long serverTimestamp = initialHandle.getServerTimestamp();
        // Now run the plan on each bucket and keep track of the total number of rows returned.
        // This uses a thread pool solely to improve the performance of the test.
        ExecutorService executor = Executors.newFixedThreadPool(planAnalysis.getAllBuckets().size());
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger returnedRowCount = new AtomicInteger();
        List<String> names = new ArrayList<>();
        for (PlanAnalysis.Partition partition : planAnalysis.getPartitions()) {
            for (PlanAnalysis.Bucket bucket : partition.getBuckets()) {
                List<String> bucketNames = new ArrayList<>();
                futures.add(executor.submit(() -> {
                    JacksonHandle resultHandle = new JacksonHandle();
                    resultHandle.setPointInTimeQueryTimestamp(serverTimestamp);
                    JsonNode result = runPlan(planAnalysis, bucket, resultHandle);
                    // It's fine if a bucket has no rows, in which case the result will be null
                    if (result != null) {
                        JsonNode rows = result.get("rows");
                        for (int i = 0; i < rows.size(); i++) {
                            String name = rows.get(i).get("Medical.Authors.LastName").get("value").asText();
                            names.add(name);
                            bucketNames.add(name + ":" + rows.get(i).get("Medical.Authors.rowid").get("value").asText());
                        }
                        returnedRowCount.addAndGet(rows.size());
                    }
                }));
            }
        }

        // Wait for all the threads to finish
        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception ex) {
                // Ignore
            }
        });

        assertEquals(15, returnedRowCount.get(),
            "All 15 author rows should have been returned; we can't assume how many will be in a bucket since the " +
                "row ID of each row is random, we just know we should get 15 back.");
    }

    private JsonNode runPlan(PlanAnalysis plan, PlanAnalysis.Bucket bucket, JacksonHandle resultHandle) {
        return rowManager.resultDoc(
            rowManager.newRawPlanDefinition(new JacksonHandle(plan.getSerializedPlan()))
                .bindParam("ML_LOWER_BOUND", bucket.lowerBound)
                .bindParam("ML_UPPER_BOUND", bucket.upperBound),
            resultHandle
        ).get();
    }
}
