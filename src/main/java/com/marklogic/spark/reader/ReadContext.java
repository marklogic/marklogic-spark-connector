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
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Captures state - all of which is serializable - that can be calculated at different times based on a user's inputs.
 * Also simplifies passing state around to the various Spark-required classes, as we only need one argument instead of
 * N arguments.
 */
class ReadContext implements Serializable {

    final static long serialVersionUID = 1;

    private final static Logger logger = LoggerFactory.getLogger(ReadContext.class);
    private final static int DEFAULT_BATCH_SIZE = 10000;

    private final Map<String, String> properties;
    private PlanAnalysis planAnalysis;
    private StructType schema;

    ReadContext(Map<String, String> properties, StructType schema) {
        this(properties);
        this.schema = schema;
    }

    /**
     * Sure seems that as soon as we create this, we should try to connect
     * to MarkLogic and analyze the user's plan.
     *
     * @param properties
     */
    ReadContext(Map<String, String> properties) {
        this.properties = properties;

        int partitionCount = getNumericOption("marklogic.num_partitions", SparkSession.active().sparkContext().defaultMinPartitions());
        int batchSize = getNumericOption("marklogic.batch_size", DEFAULT_BATCH_SIZE);
        String dslQuery = properties.get("marklogic.optic_dsl");

        DatabaseClient client = connectToMarkLogic();
        RawQueryDSLPlan dslPlan = client.newRowManager().newRawQueryDSLPlan(new StringHandle(dslQuery));

        try {
            this.planAnalysis = new PlanAnalyzer((DatabaseClientImpl) client).analyzePlan(
                dslPlan.getHandle(), partitionCount, batchSize
            );
        } catch (FailedRequestException ex) {
            handlePlanAnalysisError(dslQuery, ex);
        }
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

    DatabaseClient connectToMarkLogic() {
        DatabaseClient client = DatabaseClientFactory.newClient(propertyName -> properties.get(propertyName));
        DatabaseClient.ConnectionResult result = client.checkConnection();
        if (!result.isConnected()) {
            throw new RuntimeException(String.format("Unable to connect to MarkLogic; status code: %d; error message: %s", result.getStatusCode(), result.getErrorMessage()));
        }
        return client;
    }

    StructType getSchema() {
        return schema;
    }

    PlanAnalysis getPlanAnalysis() {
        return planAnalysis;
    }
}
