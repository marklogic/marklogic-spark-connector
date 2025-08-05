/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */

package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.ReadProgressLogger;
import com.marklogic.spark.reader.JsonRowDeserializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;

class OpticPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(OpticPartitionReader.class);

    private final OpticReadContext opticReadContext;
    private final PlanAnalysis.Partition partition;
    private final RowManager rowManager;

    private JsonRowDeserializer jsonRowDeserializer;

    private Iterator<JsonNode> rowIterator;
    private int nextBucketIndex;
    private int currentBucketRowCount;

    // Used solely for logging metrics
    private long totalRowCount;
    private long totalDuration;
    private long progressCounter;
    private final long batchSize;

    // Used solely for testing purposes; is never expected to be used in production. Intended to provide a way for
    // a test to get the count of rows returned from MarkLogic, which is important for ensuring that pushdown operations
    // are working correctly.
    static Consumer<Long> totalRowCountListener;

    OpticPartitionReader(OpticReadContext opticReadContext, PlanAnalysis.Partition partition) {
        this.opticReadContext = opticReadContext;
        this.batchSize = opticReadContext.getBatchSize();
        this.partition = partition;
        this.rowManager = opticReadContext.connectToMarkLogic().newRowManager();
        // Nested values won't work with the JacksonParser used by JsonRowDeserializer, so we ask for type info to not
        // be in the rows.
        this.rowManager.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        this.jsonRowDeserializer = new JsonRowDeserializer(opticReadContext.getSchema());
    }

    @Override
    public boolean next() {
        if (rowIterator != null) {
            if (rowIterator.hasNext()) {
                return true;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Count of rows for partition {} and bucket {}: {}", this.partition,
                        this.partition.getBuckets().get(nextBucketIndex - 1), currentBucketRowCount);
                }
                currentBucketRowCount = 0;
            }
        }

        // Iterate through buckets until we find one with at least one row.
        while (true) {
            boolean noBucketsLeftToQuery = nextBucketIndex == partition.getBuckets().size();
            if (noBucketsLeftToQuery) {
                return false;
            }

            PlanAnalysis.Bucket bucket = partition.getBuckets().get(nextBucketIndex);
            nextBucketIndex++;
            long start = System.currentTimeMillis();
            this.rowIterator = opticReadContext.readRowsInBucket(rowManager, partition, bucket);
            if (logger.isDebugEnabled()) {
                this.totalDuration += System.currentTimeMillis() - start;
            }
            boolean bucketHasAtLeastOneRow = this.rowIterator.hasNext();
            if (bucketHasAtLeastOneRow) {
                return true;
            }
        }
    }

    @Override
    public InternalRow get() {
        this.currentBucketRowCount++;
        this.totalRowCount++;
        this.progressCounter++;
        if (this.progressCounter >= this.batchSize) {
            ReadProgressLogger.logProgressIfNecessary(this.progressCounter);
            this.progressCounter = 0;
        }
        JsonNode row = rowIterator.next();
        return this.jsonRowDeserializer.deserializeJson(row.toString());
    }

    @Override
    public void close() {
        if (totalRowCountListener != null) {
            totalRowCountListener.accept(totalRowCount);
        }

        // Not yet certain how to make use of CustomTaskMetric, so just logging metrics of interest for now.
        logMetrics();
    }

    private void logMetrics() {
        if (logger.isDebugEnabled()) {
            double rowsPerSecond = totalRowCount > 0 ? totalRowCount / ((double) totalDuration / 1000) : 0;
            ObjectNode metrics = new ObjectMapper().createObjectNode()
                .put("partitionId", this.partition.getIdentifier())
                .put("totalRequests", this.partition.getBuckets().size())
                .put("totalRowCount", this.totalRowCount)
                .put("totalDuration", this.totalDuration)
                .put("rowsPerSecond", String.format("%.2f", rowsPerSecond));
            logger.debug(metrics.toString());
        }
    }
}
