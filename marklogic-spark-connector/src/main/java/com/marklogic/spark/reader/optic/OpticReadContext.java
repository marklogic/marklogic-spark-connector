/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.filter.OpticFilter;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Stream;

/**
 * Captures state - all of which is serializable - associated with a read operation involving an Optic query.
 * Also simplifies passing state around to the various Spark-required classes, as we only need one argument instead of
 * N arguments.
 */
public class OpticReadContext extends ContextSupport {

    static final long serialVersionUID = 1;

    // The ideal batch size depends highly on what a user chooses to do after a load() - and of course the user may
    // choose to perform multiple operations on the dataset, each of which may benefit from a fairly different batch
    // size. 100k has been chosen as the default batch size to strike a reasonable balance for operations that do need
    // to collect all the rows, such as writing the dataset to another data source.
    public static final long DEFAULT_BATCH_SIZE = 100000;

    private PlanAnalysis planAnalysis;
    private StructType schema;
    private List<OpticFilter> opticFilters;
    private final long batchSize;

    public OpticReadContext(Map<String, String> properties, StructType schema, int defaultMinPartitions) {
        super(properties);

        final String dslQuery = properties.get(Options.READ_OPTIC_QUERY);
        if (dslQuery == null || dslQuery.trim().isEmpty()) {
            throw new ConnectorException(Util.getOptionNameForErrorMessage("spark.marklogic.read.noOpticQuery"));
        }

        this.schema = schema;
        this.batchSize = getNumericOption(Options.READ_BATCH_SIZE, DEFAULT_BATCH_SIZE, 0);
        this.planAnalysis = analyzePlan(dslQuery, getNumericOption(Options.READ_NUM_PARTITIONS, defaultMinPartitions, 1));

        if (this.planAnalysis != null) {
            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Partition count: {}; number of requests that will be made to MarkLogic: {}",
                    this.planAnalysis.getPartitions().size(), this.planAnalysis.getAllBuckets().size());
            }
            if (Util.MAIN_LOGGER.isDebugEnabled() && planAnalysis.getServerTimestamp() > 0) {
                Util.MAIN_LOGGER.debug("Will use server timestamp: {}", planAnalysis.getServerTimestamp());
            }
        }
    }

    private PlanAnalysis analyzePlan(final String dslQuery, final long partitionCount) {
        DatabaseClient client = null;
        try {
            client = connectToMarkLogic();
            return new PlanAnalyzer((DatabaseClientImpl) client).analyzePlan(dslQuery, partitionCount, batchSize);
        } catch (FailedRequestException ex) {
            handlePlanAnalysisError(dslQuery, ex);
            return null;
        } finally {
            if (client != null) {
                client.release();
            }
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
        final long serverTimestamp = planAnalysis.getServerTimestamp();
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            if (serverTimestamp > 0) {
                Util.MAIN_LOGGER.debug("Getting rows for partition {} and bucket {} at server timestamp {}", partition, bucket, serverTimestamp);
            } else {
                Util.MAIN_LOGGER.debug("Getting rows for partition {} and bucket {}", partition, bucket);
            }
        }

        // This should never occur, as a query should only ever occur when rows were initially found, which leads to a
        // server timestamp being captured. But if it were somehow to occur, we should error out as the row-ID-based
        // partitions are not reliable without a consistent server timestamp.
        if (serverTimestamp < 1 && !bucket.isSingleCallToMarkLogic()) {
            throw new ConnectorException(String.format("Unable to read rows; invalid server timestamp: %d", serverTimestamp));
        }

        final PlanBuilder.Plan plan = buildPlanForBucket(rowManager, bucket);
        final JacksonHandle jsonHandle = new JacksonHandle();
        if (!bucket.isSingleCallToMarkLogic()) {
            jsonHandle.setPointInTimeQueryTimestamp(serverTimestamp);
        }

        // Remarkably, the use of resultDoc has consistently proven to be a few percentage points faster than using
        // resultRows with a StringHandle, even though the latter avoids the need for converting to and from a JsonNode.
        // The overhead with resultRows may be due to the processing of a multipart response; it's not yet clear.
        final JsonNode result = rowManager.resultDoc(plan, jsonHandle).get();
        return result != null && result.has("rows") ?
            result.get("rows").iterator() :
            new ArrayList<JsonNode>().iterator();
    }

    private PlanBuilder.Plan buildPlanForBucket(RowManager rowManager, PlanAnalysis.Bucket bucket) {
        PlanBuilder.Plan plan = rowManager.newRawPlanDefinition(new JacksonHandle(planAnalysis.getSerializedPlan()));

        if (!bucket.isSingleCallToMarkLogic()) {
            plan = plan.bindParam("ML_LOWER_BOUND", bucket.lowerBound)
                .bindParam("ML_UPPER_BOUND", bucket.upperBound);
        }

        if (opticFilters != null) {
            for (OpticFilter opticFilter : opticFilters) {
                plan = opticFilter.bindFilterValue(plan);
            }
        }

        return plan;
    }

    void pushDownFiltersIntoOpticQuery(List<OpticFilter> opticFilters) {
        this.opticFilters = opticFilters;
        // Add each filter in a separate "where" so we don't toss an op.sqlCondition into an op.and,
        // which Optic does not allow.
        opticFilters.forEach(filter -> planAnalysis.pushOperatorIntoPlan(PlanUtil.buildWhere(filter)));
    }

    void pushDownLimit(int limit) {
        planAnalysis.pushOperatorIntoPlan(PlanUtil.buildLimit(limit));
    }

    void pushDownTopN(SortOrder[] orders, int limit) {
        planAnalysis.pushOperatorIntoPlan(PlanUtil.buildOrderBy(orders));
        pushDownLimit(limit);
    }

    void pushDownAggregation(Aggregation aggregation) {
        final List<String> groupByColumnNames = Stream.of(aggregation.groupByExpressions())
            .map(PlanUtil::expressionToColumnName)
            .toList();

        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("groupBy column names: {}", groupByColumnNames);
        }
        planAnalysis.pushOperatorIntoPlan(PlanUtil.buildGroupByAggregation(new HashSet<>(groupByColumnNames), aggregation));

        StructType newSchema = buildSchemaWithColumnNames(groupByColumnNames);

        for (AggregateFunc func : aggregation.aggregateExpressions()) {
            if (func instanceof Avg) {
                newSchema = newSchema.add(func.toString(), DataTypes.DoubleType);
            } else if (func instanceof Count) {
                newSchema = newSchema.add(func.toString(), DataTypes.LongType);
            } else if (func instanceof CountStar) {
                newSchema = newSchema.add("count", DataTypes.LongType);
            } else if (func instanceof Max max) {
                StructField field = findColumnInSchema(max.column(), PlanUtil.expressionToColumnName(max.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else if (func instanceof Min min) {
                StructField field = findColumnInSchema(min.column(), PlanUtil.expressionToColumnName(min.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else if (func instanceof Sum sum) {
                StructField field = findColumnInSchema(sum.column(), PlanUtil.expressionToColumnName(sum.column()));
                newSchema = newSchema.add(func.toString(), field.dataType());
            } else if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Unsupported aggregate function: {}", func);
            }
        }

        if (!getProperties().containsKey(Options.READ_BATCH_SIZE)) {
            Util.MAIN_LOGGER.info("Batch size was not overridden, so modifying each partition to make a single request to improve " +
                "performance of pushed down aggregation.");
            List<PlanAnalysis.Partition> mergedPartitions = planAnalysis.getPartitions().stream()
                .map(p -> p.mergeBuckets())
                .toList();
            this.planAnalysis = new PlanAnalysis(planAnalysis.getSerializedPlan(), mergedPartitions, planAnalysis.getServerTimestamp());
        }

        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Schema after pushing down aggregation: {}", newSchema);
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
        planAnalysis.pushOperatorIntoPlan(PlanUtil.buildSelect(requiredSchema));
    }

    boolean planAnalysisFoundNoRows() {
        // The planAnalysis will be null if no rows were found, which internal/viewinfo unfortunately throws an error
        // on. None of the push down operations need to be applied in this scenario.
        return planAnalysis == null;
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

    long getBatchSize() {
        return batchSize;
    }
}
