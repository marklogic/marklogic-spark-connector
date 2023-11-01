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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.row.RowManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;

class MarkLogicPartitionReader implements PartitionReader {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicPartitionReader.class);

    private final ReadContext readContext;
    private final PlanAnalysis.Partition partition;
    private final RowManager rowManager;

    private JsonRowDeserializer jsonRowDeserializer;

    private Iterator<JsonNode> rowIterator;
    private int nextBucketIndex;
    private int currentBucketRowCount;

    // Used solely for logging metrics
    private long totalRowCount;
    private long totalDuration;

    // Used solely for testing purposes; is never expected to be used in production. Intended to provide a way for
    // a test to get the count of rows returned from MarkLogic, which is important for ensuring that pushdown operations
    // are working correctly.
    static Consumer<Long> totalRowCountListener;

    MarkLogicPartitionReader(ReadContext readContext, PlanAnalysis.Partition partition) {
        this.readContext = readContext;
        this.partition = partition;
        this.rowManager = readContext.connectToMarkLogic().newRowManager();
        // Nested values won't work with the JacksonParser used by JsonRowDeserializer, so we ask for type info to not
        // be in the rows.
        this.rowManager.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        this.jsonRowDeserializer = new JsonRowDeserializer(readContext.getSchema());
    }

    @Override
    public boolean next() {
        if (rowIterator != null) {
            if (rowIterator.hasNext()) {
                return true;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Count of rows for partition {} and bucket {}: {}", this.partition,
                        this.partition.buckets.get(nextBucketIndex - 1), currentBucketRowCount);
                }
                currentBucketRowCount = 0;
            }
        }

        // Iterate through buckets until we find one with at least one row.
        while (true) {
            boolean noBucketsLeftToQuery = nextBucketIndex == partition.buckets.size();
            if (noBucketsLeftToQuery) {
                return false;
            }

            PlanAnalysis.Bucket bucket = partition.buckets.get(nextBucketIndex);
            nextBucketIndex++;
            long start = System.currentTimeMillis();
            this.rowIterator = readContext.readRowsInBucket(rowManager, partition, bucket);
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
            double rowsPerSecond = totalRowCount > 0 ? (double) totalRowCount / ((double) totalDuration / 1000) : 0;
            ObjectNode metrics = new ObjectMapper().createObjectNode()
                .put("partitionId", this.partition.identifier)
                .put("totalRequests", this.partition.buckets.size())
                .put("totalRowCount", this.totalRowCount)
                .put("totalDuration", this.totalDuration)
                .put("rowsPerSecond", String.format("%.2f", rowsPerSecond));
            logger.debug(metrics.toString());
        }
    }
}
