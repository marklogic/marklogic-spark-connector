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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.StringHandle;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class MarkLogicBatch implements Batch {

    private final static int DEFAULT_BATCH_SIZE = 10000;
    private final static Logger logger = LoggerFactory.getLogger(MarkLogicBatch.class);

    private final StructType schema;
    private final Map<String, String> properties;
    private PlanAnalysis planAnalysis;

    MarkLogicBatch(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;

        int partitionCount = getNumericOption("marklogic.num_partitions", SparkSession.active().sparkContext().defaultMinPartitions());
        int batchSize = getNumericOption("marklogic.batch_size", DEFAULT_BATCH_SIZE);
        String query = properties.get("marklogic.optic_dsl");

        DatabaseClient client = ClientUtil.connectToMarkLogic(properties);
        try {
            this.planAnalysis = new PlanAnalyzer((DatabaseClientImpl) client).analyzePlan(
                client.newRowManager().newRawQueryDSLPlan(new StringHandle(query)).getHandle(),
                partitionCount, batchSize
            );
        } catch (FailedRequestException ex) {
            handlePlanAnalysisError(query, ex);
        }
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return this.planAnalysis != null ?
            this.planAnalysis.getPartitionArray() :
            new InputPartition[]{};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new MarkLogicPartitionReaderFactory(this.planAnalysis, this.schema, this.properties);
    }

    private int getNumericOption(String optionName, int defaultValue) {
        try {
            int value = this.properties.containsKey(optionName) ?
                Integer.parseInt(this.properties.get(optionName)) :
                defaultValue;
            if (value < 1) {
                throw new IllegalArgumentException(String.format("Value of '%s' option must be 1 or greater", optionName));
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(String.format("Value of '%s' option must be numeric", optionName), ex);
        }
    }

    private void handlePlanAnalysisError(String query, FailedRequestException ex) {
        final String indicatorOfNoRowsExisting = "$tableId as xs:string -- Invalid coercion: () as xs:string";
        if (ex.getMessage().contains(indicatorOfNoRowsExisting)) {
            logger.info("No rows were found, so will not create any partitions.");
        } else {
            throw new RuntimeException(String.format("Unable to run Optic DSL query %s; cause: %s", query, ex.getMessage()), ex);
        }
    }
}
