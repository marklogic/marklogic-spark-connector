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
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.ArrayList;
import java.util.List;

/**
 * "Analyze" = take a user's plan (from their Optic DSL query) and parameterize it with lower and upper bounds,
 * and also calculate partitions.
 */
class PlanAnalyzer {

    private DatabaseClientImpl databaseClient;

    PlanAnalyzer(DatabaseClientImpl databaseClient) {
        this.databaseClient = databaseClient;
    }

    PlanAnalysis analyzePlan(AbstractWriteHandle userPlan, int userPartitionCount, int userBatchSize) {
        JsonNode viewInfo = databaseClient.getServices().postResource(
            null, "internal/viewinfo", null, null, userPlan, new JacksonHandle()
        ).get();

        long rowCount = viewInfo.get("rowCount").asLong(0);
        List<PlanAnalysis.Partition> partitions = calculatePartitions(rowCount, userPartitionCount, userBatchSize);
        return new PlanAnalysis(viewInfo.get("modifiedPlan"), partitions);
    }

    private List<PlanAnalysis.Partition> calculatePartitions(long rowCount, int userPartitionCount, int userBatchSize) {
        long bucketCount = (rowCount / userPartitionCount) / userBatchSize;
        if (bucketCount < 1) {
            bucketCount = 1;
        }
        long partitionSize = Long.divideUnsigned(-1, userPartitionCount);
        long nextLowerBound = 0;

        List<PlanAnalysis.Partition> partitions = new ArrayList<>();
        for (int i = 1; i <= userPartitionCount; i++) {
            long upperBound = (i == userPartitionCount) ? -1 : nextLowerBound + partitionSize;
            partitions.add(new PlanAnalysis.Partition(i, nextLowerBound, upperBound, bucketCount, partitionSize));
            nextLowerBound = nextLowerBound + partitionSize + 1;
        }
        return partitions;
    }
}
