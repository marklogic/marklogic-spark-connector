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
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
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
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.filter.OpticFilter;
import com.marklogic.spark.reader.filter.SingleValueFilter;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Captures state - all of which is serializable - associated with a read operation involving an Optic query.
 * Also simplifies passing state around to the various Spark-required classes, as we only need one argument instead of
 * N arguments.
 */
public class OpticReadContext extends ContextSupport {

    static final long serialVersionUID = 1;

    private static final Logger logger = LoggerFactory.getLogger(OpticReadContext.class);

    // The ideal batch size depends highly on what a user chooses to do after a load() - and of course the user may
    // choose to perform multiple operations on the dataset, each of which may benefit from a fairly different batch
    // size. 100k has been chosen as the default batch size to strike a reasonable balance for operations that do need
    // to collect all the rows, such as writing the dataset to another data source.
    private static final long DEFAULT_BATCH_SIZE = 100000;

    private PlanAnalysis planAnalysis;
    private StructType schema;
    private long serverTimestamp;
    private boolean useServerTimestamp;
    private List<OpticFilter> opticFilters;
    private final Map<String, String> userDefinedParams;
    private final Map<String, String> columnParamDefaults;
    private Map<String, String> columnParamValues;

    public OpticReadContext(Map<String, String> properties, StructType schema, int defaultMinPartitions) {
        super(properties);
        this.schema = schema;

        // There's a defect in ML server that causes queries with timestamps to not return correct errors.
        // This allows the timestamp to be supressed for debugging purposes.
        this.useServerTimestamp = !properties.containsKey(Options.READ_DISABLE_TIMESTAMP) ||
            !properties.get(Options.READ_DISABLE_TIMESTAMP).trim().toLowerCase().equals("true");

        // Stash a map of the user-defined parameters (if any) so that they aren't calculated on every call.
        this.userDefinedParams = properties.keySet().stream()
            .filter(key -> key.startsWith(Options.READ_PARAMS_PREFIX))
            .collect(Collectors.toMap(
                key -> key.substring(Options.READ_PARAMS_PREFIX.length()),
                properties::get
            ));

        if (logger.isDebugEnabled()) {
            logger.debug("Using user params: {}", userDefinedParams);
        }

        this.columnParamDefaults = properties.keySet().stream()
            .filter(key -> key.startsWith(Options.READ_COLUMN_PARAMS_PREFIX))
            .collect(Collectors.toMap(
                key -> key.substring(Options.READ_COLUMN_PARAMS_PREFIX.length()),
                properties::get
            ));

        // This is necessary because Collectors.toMap doesn't support null.
        // See https://stackoverflow.com/questions/24630963/nullpointerexception-in-collectors-tomap-with-null-entry-values
        this.columnParamValues = columnParamDefaults.keySet().stream()
            .collect(HashMap::new, (map, value) -> map.put(value, null), HashMap::putAll);

        if (logger.isDebugEnabled()) {
            logger.debug("Using param columns: {}", columnParamDefaults.keySet());
        }

        final long partitionCount = getNumericOption(Options.READ_NUM_PARTITIONS, defaultMinPartitions, 1);
        final long batchSize = getNumericOption(Options.READ_BATCH_SIZE, DEFAULT_BATCH_SIZE, 0);

        final String dslQuery = properties.get(Options.READ_OPTIC_QUERY);
        if (dslQuery == null || dslQuery.trim().length() < 1) {
            throw new IllegalArgumentException(String.format("No Optic query found; must define %s", Options.READ_OPTIC_QUERY));
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
            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Partition count: {}; number of requests that will be made to MarkLogic: {}",
                    this.planAnalysis.getPartitions().size(), this.planAnalysis.getAllBuckets().size());
            }

            if (useServerTimestamp) {
                // Calling this to establish a server timestamp.
                StringHandle columnInfoHandle = client.newRowManager().columnInfo(dslPlan, new StringHandle());
                this.serverTimestamp = columnInfoHandle.getServerTimestamp();
                if (logger.isDebugEnabled()) {
                    logger.debug("Will use server timestamp: {}", serverTimestamp);
                }
            } else if (logger.isWarnEnabled()) {
                logger.warn("Server timestamp disabled - results are not guaranteed to be consistent!!!");
            }
        }

        // Param columns need to be added before filters
        // TODO: this can be done as a single bind operation with an array
        for (String key : columnParamValues.keySet()) {
            addOperatorToPlan(PlanUtil.buildBind(key, PlanUtil.buildParam(key)));
        }
    }

    private void handlePlanAnalysisError(String query, FailedRequestException ex) {
        final String indicatorOfNoRowsExisting = "$tableId as xs:string -- Invalid coercion: () as xs:string";
        if (ex.getMessage().contains(indicatorOfNoRowsExisting)) {
            Util.MAIN_LOGGER.info("No rows were found, so will not create any partitions.");
        } else {
            throw new ConnectorException(String.format("Unable to run Optic query %s; cause: %s", query, ex.getMessage()), ex);
        }
    }

    Iterator<JsonNode> readRowsInBucket(RowManager rowManager, PlanAnalysis.Partition partition, PlanAnalysis.Bucket bucket) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting rows for partition {} and bucket {} at server timestamp {}", partition, bucket, serverTimestamp);
        }

        // This should never occur, as a query should only ever occur when rows were initially found, which leads to a
        // server timestamp being captured. But if it were somehow to occur, we should error out as the row-ID-based
        // partitions are not reliable without a consistent server timestamp.
        if (useServerTimestamp && serverTimestamp < 1) {
            throw new ConnectorException(String.format("Unable to read rows; invalid server timestamp: %d", serverTimestamp));
        }

        PlanBuilder.Plan plan = buildPlanForBucket(rowManager, bucket);

        JacksonHandle jsonHandle = new JacksonHandle();

        if (useServerTimestamp) {
            jsonHandle.setPointInTimeQueryTimestamp(serverTimestamp);
        }
        
        // Remarkably, the use of resultDoc has consistently proven to be a few percentage points faster than using
        // resultRows with a StringHandle, even though the latter avoids the need for converting to and from a JsonNode.
        // The overhead with resultRows may be due to the processing of a multipart response; it's not yet clear.
        JsonNode result = rowManager.resultDoc(plan, jsonHandle).get();
        return result != null && result.has("rows") ?
            result.get("rows").iterator() :
            new ArrayList<JsonNode>().iterator();
    }

    private PlanBuilder.Plan buildPlanForBucket(RowManager rowManager, PlanAnalysis.Bucket bucket) {
        PlanBuilder.Plan plan = rowManager.newRawPlanDefinition(new JacksonHandle(planAnalysis.getBoundedPlan()))
            .bindParam("ML_LOWER_BOUND", bucket.lowerBound)
            .bindParam("ML_UPPER_BOUND", bucket.upperBound);

        // Add user-defined parameters from options
        for(Map.Entry<String, String> entry : this.userDefinedParams.entrySet()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Adding user param: {}", entry);
            }
            plan = plan.bindParam(entry.getKey(), entry.getValue());
        }

        if (opticFilters != null) {
            for (OpticFilter opticFilter : opticFilters) {
                plan = opticFilter.bindFilterValue(plan);
            }
        }

        for (Map.Entry<String, String> entry : columnParamValues.entrySet()) {
            if (entry.getValue() == null) {
                entry.setValue(columnParamDefaults.get(entry.getKey()));
                if (logger.isDebugEnabled()) {
                    logger.debug("Setting default param column: {}", entry);
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Setting param column: {}", entry);
            }

            plan = plan.bindParam(entry.getKey(), entry.getValue());
        }

        return plan;
    }

    void pushDownFiltersIntoOpticQuery(List<OpticFilter> opticFilters) {
        this.opticFilters = opticFilters;
        // Add each filter in a separate "where" so we don't toss an op.sqlCondition into an op.and,
        // which Optic does not allow.
        opticFilters.forEach(filter -> {
            // If the filter is a dynamic variable, we need to assign it
            if (
                filter instanceof SingleValueFilter &&
                columnParamValues.containsKey(((SingleValueFilter)filter).getColumnName())
            ) {
                // TODO: support types other than string
                columnParamValues.put(
                    ((SingleValueFilter)filter).getColumnName(),
                    ((SingleValueFilter)filter).getCompareValue().toString()
                );
            }

            addOperatorToPlan(PlanUtil.buildWhere(filter));
        });
    }

    void pushDownLimit(int limit) {
        addOperatorToPlan(PlanUtil.buildLimit(limit));
    }

    void pushDownTopN(SortOrder[] orders, int limit) {
        addOperatorToPlan(PlanUtil.buildOrderBy(orders));
        pushDownLimit(limit);
    }

    void pushDownAggregation(Aggregation aggregation) {
        final List<String> groupByColumnNames = Stream.of(aggregation.groupByExpressions())
            .map(PlanUtil::expressionToColumnName)
            .collect(Collectors.toList());

        addOperatorToPlan(PlanUtil.buildGroupByAggregation(groupByColumnNames, aggregation));

        StructType newSchema = buildSchemaWithColumnNames(groupByColumnNames);

        for (AggregateFunc func : aggregation.aggregateExpressions()) {
            if (func instanceof Avg) {
                newSchema = newSchema.add(func.toString(), DataTypes.DoubleType);
            } else if (func instanceof Count) {
                newSchema = newSchema.add(func.toString(), DataTypes.LongType);
            } else if (func instanceof CountStar) {
                newSchema = newSchema.add("count", DataTypes.LongType);
            } else if (func instanceof Max) {
                Max max = (Max) func;
                StructField field = findColumnInSchema(max.column(), PlanUtil.expressionToColumnName(max.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else if (func instanceof Min) {
                Min min = (Min) func;
                StructField field = findColumnInSchema(min.column(), PlanUtil.expressionToColumnName(min.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else if (func instanceof Sum) {
                Sum sum = (Sum) func;
                StructField field = findColumnInSchema(sum.column(), PlanUtil.expressionToColumnName(sum.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Unsupported aggregate function: {}", func);
                }
            }
        }

        if (!getProperties().containsKey(Options.READ_BATCH_SIZE)) {
            Util.MAIN_LOGGER.info("Batch size was not overridden, so modifying each partition to make a single request to improve " +
                "performance of pushed down aggregation.");
            List<PlanAnalysis.Partition> mergedPartitions = planAnalysis.getPartitions().stream()
                .map(p -> p.mergeBuckets())
                .collect(Collectors.toList());
            this.planAnalysis = new PlanAnalysis(planAnalysis.getBoundedPlan(), mergedPartitions);
        }

        this.schema = newSchema;
    }

    private StructType buildSchemaWithColumnNames(List<String> groupByColumnNames) {
        StructType newSchema = new StructType();
        for (String columnName : groupByColumnNames) {
            StructField columnField = null;
            for (StructField field : this.schema.fields()) {
                if (columnName.equals(field.name())) {
                    columnField = field;
                    break;
                }
            }
            if (columnField == null) {
                throw new IllegalArgumentException("Unable to find column in schema; column name: " + columnName);
            }
            newSchema = newSchema.add(columnField);
        }
        return newSchema;
    }

    private StructField findColumnInSchema(Expression expression, String columnName) {
        for (StructField field : this.schema.fields()) {
            if (columnName.equals(field.name())) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unable to find column in schema for expression: " + expression.describe());
    }

    void pushDownRequiredSchema(StructType requiredSchema) {
        this.schema = requiredSchema;
        addOperatorToPlan(PlanUtil.buildSelect(requiredSchema));
    }

    boolean planAnalysisFoundNoRows() {
        // The planAnalysis will be null if no rows were found, which internal/viewinfo unfortunately throws an error
        // on. None of the push down operations need to be applied in this scenario.
        return planAnalysis == null;
    }

    /**
     * The internal/viewinfo endpoint is known to add an op:prepare operator at the end of the list of operator
     * args. Each operator added by the connector based on pushdowns needs to be added before this op:prepare
     * operator; otherwise, MarkLogic will throw an error.
     *
     * @param operator
     */
    private void addOperatorToPlan(ObjectNode operator) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding operator to plan: {}", operator);
        }
        ArrayNode operators = (ArrayNode) planAnalysis.getBoundedPlan().get("$optic").get("args");
        operators.insert(operators.size() - 1, operator);
    }

    StructType getSchema() {
        return schema;
    }

    PlanAnalysis getPlanAnalysis() {
        return planAnalysis;
    }

    long getBucketCount() {
        return planAnalysis != null ? planAnalysis.getAllBuckets().size() : 0;
    }
}
