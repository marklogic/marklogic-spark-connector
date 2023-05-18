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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.filter.OpticFilter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    private final static ObjectMapper objectMapper = new ObjectMapper();

    private PlanAnalysis planAnalysis;
    private StructType schema;
    private long serverTimestamp;
    private List<OpticFilter> opticFilters;

    public ReadContext(Map<String, String> properties, StructType schema) {
        super(properties);
        this.schema = schema;

        final long partitionCount = getNumericOption(Options.READ_NUM_PARTITIONS,
            SparkSession.active().sparkContext().defaultMinPartitions(), 1);
        final long batchSize = getNumericOption(Options.READ_BATCH_SIZE, DEFAULT_BATCH_SIZE, 0);
        final String dslQuery = properties.get(Options.READ_OPTIC_DSL);
        if (dslQuery == null || dslQuery.trim().length() < 1) {
            throw new IllegalArgumentException(String.format("No Optic query found; must define %s", Options.READ_OPTIC_DSL));
        }
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
            // Calling this to establish a server timestamp.
            StringHandle columnInfoHandle = client.newRowManager().columnInfo(dslPlan, new StringHandle());
            this.serverTimestamp = columnInfoHandle.getServerTimestamp();
            if (logger.isDebugEnabled()) {
                logger.debug("Will use server timestamp: {}", serverTimestamp);
            }
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

    Iterator<JsonNode> readRowsInBucket(RowManager rowManager, PlanAnalysis.Partition partition, PlanAnalysis.Bucket bucket) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting rows for partition {} and bucket {} at server timestamp {}", partition, bucket, serverTimestamp);
        }

        // This should never occur, as a query should only ever occur when rows were initially found, which leads to a
        // server timestamp being captured. But if it were somehow to occur, we should error out as the row-ID-based
        // partitions are not reliable without a consistent server timestamp.
        if (serverTimestamp < 1) {
            throw new RuntimeException(String.format("Unable to read rows; invalid server timestamp: %d", serverTimestamp));
        }

        PlanBuilder.Plan plan = buildPlanForBucket(rowManager, bucket);
        JacksonHandle jsonHandle = new JacksonHandle();
        jsonHandle.setPointInTimeQueryTimestamp(serverTimestamp);
        // Unable to use resultRows as Java Client <= 6.2.0 has a bug where the serverTimestamp is ignored.
        // TODO Change this to use resultRows once Java Client 6.2.1 is available. That should perform better as it
        // avoids creating a JsonNode.
        JsonNode result = rowManager.resultDoc(plan, jsonHandle).get();
        return result != null && result.has("rows") ?
            result.get("rows").iterator() :
            new ArrayList<JsonNode>().iterator();
    }

    private PlanBuilder.Plan buildPlanForBucket(RowManager rowManager, PlanAnalysis.Bucket bucket) {
        PlanBuilder.Plan plan = rowManager.newRawPlanDefinition(new JacksonHandle(planAnalysis.boundedPlan))
            .bindParam("ML_LOWER_BOUND", bucket.lowerBound)
            .bindParam("ML_UPPER_BOUND", bucket.upperBound);

        if (opticFilters != null) {
            for (OpticFilter opticFilter : opticFilters) {
                plan = opticFilter.bindFilterValue(plan);
            }
        }

        return plan;
    }

    void pushDownFiltersIntoOpticQuery(List<OpticFilter> opticFilters) {
        this.opticFilters = opticFilters;

        // All the filters will be added to a new "where" clause. And because the internal/viewinfo endpoint is known
        // to place an op:prepare call as the last arg (which apparently needs to be last), the new 'where' clause is
        // inserted before the op:prepare call.
        final ObjectNode whereClause = objectMapper.createObjectNode().put("ns", "op").put("fn", "where");
        final ArrayNode operators = (ArrayNode) planAnalysis.boundedPlan.get("$optic").get("args");
        operators.insert(operators.size() - 1, whereClause);

        // If there's only one filter, can toss it into the "where" clause. Else, toss an "and" into the "where" and
        // then toss every filter into the "and" clause (which accepts 2 to N args).
        final ArrayNode whereArgs = whereClause.putArray("args");
        final ArrayNode targetArgs = opticFilters.size() == 1 ?
            whereArgs :
            whereArgs.addObject().put("ns", "op").put("fn", "and").putArray("args");

        opticFilters.forEach(planFilter -> planFilter.populateArg(targetArgs.addObject()));
    }

    StructType getSchema() {
        return schema;
    }

    PlanAnalysis getPlanAnalysis() {
        return planAnalysis;
    }
}
