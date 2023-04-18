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
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;


public class MarkLogicPartitionReader implements PartitionReader {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicPartitionReader.class);

    private Iterator<RowRecord> rowRecordIterator;
    private Function<Row, InternalRow> rowConverter;
    private PlanAnalysis.Partition partition;
    private int nextBucketIndex;
    private int currentBucketRowCount;
    private JsonNode boundedPlan;
    private RowManager rowManager;

    public MarkLogicPartitionReader(JsonNode boundedPlan, PlanAnalysis.Partition partition, StructType schema, Map<String, String> map) {
        this.boundedPlan = boundedPlan;
        this.partition = partition;
        this.rowManager = DatabaseClientFactory.newClient(propertyName -> map.get(propertyName)).newRowManager();
        this.rowConverter = new MarkLogicRowToInternalRowFunction(schema);
    }

    @Override
    public boolean next() {
        if (rowRecordIterator != null) {
            if (rowRecordIterator.hasNext()) {
                return true;
            } else {
                logger.debug("Count of rows for partition {} and bucket {}: {}", this.partition,
                    this.partition.buckets.get(nextBucketIndex - 1), currentBucketRowCount);
                currentBucketRowCount = 0;
            }
        }

        // Iterate through buckets until we find one with at least one row.
        while (true) {
            // If we've checked all the buckets, we're done!
            if (nextBucketIndex == partition.buckets.size()) {
                return false;
            }

            PlanAnalysis.Bucket bucket = partition.buckets.get(nextBucketIndex);
            nextBucketIndex++;

            if (logger.isDebugEnabled()) {
                logger.debug("Getting rows for partition {} and bucket {}", this.partition, bucket);
            }

            this.rowRecordIterator = rowManager.resultRows(
                rowManager.newRawPlanDefinition(new JacksonHandle(this.boundedPlan))
                    .bindParam("ML_LOWER_BOUND", bucket.lowerBound)
                    .bindParam("ML_UPPER_BOUND", bucket.upperBound)
            ).iterator();

            // If the bucket has at least one row, then use this.
            if (this.rowRecordIterator.hasNext()) {
                return true;
            }
        }
    }

    @Override
    public InternalRow get() {
        // Just doing some hacking for now, will make this "real" via DEVEXP-374.
        RowRecord rowRecord = rowRecordIterator.next();
        Row sparkRow = RowFactory.create(
            rowRecord.getInt("Medical.Authors.CitationID"),
            rowRecord.getString("Medical.Authors.LastName")
        );

        // Temporary hacking available for using PerformanceTester.
        // This will be removed once 374 is done.
//        Row sparkRow = RowFactory.create(
//            rowRecord.getInt("demo.employee.employee_id"),
//            rowRecord.getInt("demo.employee.person_id"),
//            rowRecord.getString("demo.employee.job_description")
//        );

        this.currentBucketRowCount++;
        return this.rowConverter.apply(sparkRow);
    }

    @Override
    public void close() {
    }
}
