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
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawPlanDefinition;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.ContextSupport;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Captures state - all of which is serializable - that can be calculated at different times based on a user's inputs.
 * Also simplifies passing state around to the various Spark-required classes, as we only need one argument instead of
 * N arguments.
 */
public class ReadContext extends ContextSupport {

    final static long serialVersionUID = 1;

    private final static Logger logger = LoggerFactory.getLogger(ReadContext.class);
    private final static long DEFAULT_BATCH_SIZE = 10000;

    private PlanAnalysis planAnalysis;
    private StructType schema;
    private long serverTimestamp;

    public ReadContext(Map<String, String> properties, StructType schema) {
        this(properties, false);
        this.schema = schema;
    }

    /**
     * Sure seems that as soon as we create this, we should try to connect
     * to MarkLogic and analyze the user's plan.
     *
     * @param properties
     * @param inferSchema
     */
    public ReadContext(Map<String, String> properties, boolean inferSchema) {
        super(properties);

        final long partitionCount = getNumericOption(ReadConstants.NUM_PARTITIONS,
            SparkSession.active().sparkContext().defaultMinPartitions(), 1);
        final long batchSize = getNumericOption(ReadConstants.BATCH_SIZE, DEFAULT_BATCH_SIZE, 0);
        final String dslQuery = properties.get(ReadConstants.OPTIC_DSL);

        DatabaseClient client = connectToMarkLogic();
        RawQueryDSLPlan dslPlan = client.newRowManager().newRawQueryDSLPlan(new StringHandle(dslQuery));

        try {
            this.planAnalysis = new PlanAnalyzer((DatabaseClientImpl) client).analyzePlan(
                dslPlan.getHandle(), partitionCount, batchSize
            );
        } catch (FailedRequestException ex) {
            handlePlanAnalysisError(dslQuery, ex);
        }

        if (this.planAnalysis != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Partition count: {}; number of requests that will be made to MarkLogic: {}",
                    this.planAnalysis.partitions.size(), this.planAnalysis.getAllBuckets().size());
            }
            StringHandle columnInfoHandle = client.newRowManager().columnInfo(dslPlan, new StringHandle());
            this.serverTimestamp = columnInfoHandle.getServerTimestamp();
            if (inferSchema) {
                this.schema = SchemaInferrer.inferSchema(columnInfoHandle.get());
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("Inferred schema: %s", schema.json()));
                }
            }
        } else {
            // If no rows are found, and the user doesn't provide a schema, need a non-null one to keep Spark happy.
            this.schema = new StructType();
        }
    }

    private long getNumericOption(String optionName, long defaultValue, long minimumValue) {
        try {
            long value = this.getProperties().containsKey(optionName) ?
                Long.parseLong(this.getProperties().get(optionName)) :
                defaultValue;
            if (value < minimumValue) {
                throw new IllegalArgumentException(String.format("Value of '%s' option must be %d or greater", optionName, minimumValue));
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

    Iterator<StringHandle> readRowsInBucket(RowManager rowManager, PlanAnalysis.Partition partition, PlanAnalysis.Bucket bucket) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting rows for partition {} and bucket {} at server timestamp {}", partition, bucket, serverTimestamp);
        }

        // This should never occur, as a query should only ever occur when rows were initially found, which leads to a
        // server timestamp being captured. But if it were somehow to occur, we should error out as the row-ID-based
        // partitions are not reliable without a consistent server timestamp.
        if (serverTimestamp < 1) {
            throw new RuntimeException(String.format("Unable to read rows; invalid server timestamp: %d", serverTimestamp));
        }

        JacksonHandle planHandle = new JacksonHandle(planAnalysis.boundedPlan);

        RawPlanDefinition rawPlan = rowManager.newRawPlanDefinition(planHandle);
        PlanBuilder.Plan builtPlan = rawPlan
            .bindParam("ML_LOWER_BOUND", bucket.lowerBound)
            .bindParam("ML_UPPER_BOUND", bucket.upperBound);

        StringHandle resultHandle = new StringHandle().withFormat(Format.JSON);
        resultHandle.setPointInTimeQueryTimestamp(serverTimestamp);
        return rowManager.resultRows(builtPlan, resultHandle).iterator();
    }

    public StructType getSchema() {
        return schema;
    }

    PlanAnalysis getPlanAnalysis() {
        return planAnalysis;
    }
}
