/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.reader.filter.OpticFilter;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Methods for modifying a serialized Optic plan. These were moved here both to facilitate unit testing for some of them
 * and to simplify {@code ReadContext}.
 */
public abstract class PlanUtil {

    private static final Logger logger = LoggerFactory.getLogger(PlanUtil.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static Map<Class<? extends AggregateFunc>, Function<AggregateFunc, OpticFunction>> aggregateFunctionHandlers;

    private static final String COUNT = "count";

    // Construct the mapping of Spark aggregate function instances to OpticFunction instances that are used to build
    // the corresponding serialized Optic function reference.
    static {
        aggregateFunctionHandlers = new HashMap<>();
        aggregateFunctionHandlers.put(Avg.class, func -> {
            Avg avg = (Avg) func;
            return new OpticFunction("avg", avg.column(), avg.isDistinct());
        });
        aggregateFunctionHandlers.put(Count.class, func -> {
            Count count = (Count) func;
            return new OpticFunction(COUNT, count.column(), count.isDistinct());
        });
        aggregateFunctionHandlers.put(Max.class, func -> new OpticFunction("max", ((Max) func).column()));
        aggregateFunctionHandlers.put(Min.class, func -> new OpticFunction("min", ((Min) func).column()));
        aggregateFunctionHandlers.put(Sum.class, func -> {
            Sum sum = (Sum) func;
            return new OpticFunction("sum", sum.column(), sum.isDistinct());
        });
    }

    private PlanUtil() {
    }

    /**
     * @param columnNames a set of unique column names is needed as Optic will otherwise throw an error via a
     *                    "duplicate check" per a fix for https://bugtrack.marklogic.com/56662 .
     * @param aggregation
     * @return
     */
    static ObjectNode buildGroupByAggregation(Set<String> columnNames, Aggregation aggregation) {
        return newOperation("group-by", groupByArgs -> {
            ArrayNode columns = groupByArgs.addArray();
            columnNames.forEach(columnName -> populateSchemaCol(columns.addObject(), columnName));

            ArrayNode aggregates = groupByArgs.addArray();
            for (AggregateFunc func : aggregation.aggregateExpressions()) {
                // Need special handling for CountStar, as it does not have a column name with it.
                if (func instanceof CountStar) {
                    aggregates.addObject().put("ns", "op").put("fn", COUNT).putArray("args")
                        // "count" is used as the column name as that's what Spark uses when the operation is not pushed down.
                        .add(COUNT)
                        // Using "null" is the equivalent of "count(*)" - it counts rows, not values.
                        .add(objectMapper.nullNode());
                } else if (aggregateFunctionHandlers.containsKey(func.getClass())) {
                    OpticFunction opticFunction = aggregateFunctionHandlers.get(func.getClass()).apply(func);
                    ArrayNode aggregateArgs = aggregates
                        .addObject().put("ns", "op").put("fn", opticFunction.functionName)
                        .putArray("args");
                    aggregateArgs.add(func.toString());
                    populateSchemaCol(aggregateArgs.addObject(), opticFunction.columnName);
                    // This is the correct JSON to add, but have not found a way to create an AggregateFunc that
                    // returns "true" for isDistinct().
                    if (opticFunction.distinct) {
                        aggregateArgs.addObject().put("values", "distinct");
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Unsupported aggregate function, will not be pushed to Optic: {}", func);
                    }
                }
            }
        });
    }

    static ObjectNode buildLimit(int limit) {
        return newOperation("limit", args -> args.add(limit));
    }

    static ObjectNode buildOrderBy(SortOrder[] sortOrders) {
        return newOperation("order-by", args -> {
            ArrayNode innerArgs = args.addArray();
            for (SortOrder sortOrder : sortOrders) {
                final String direction = SortDirection.ASCENDING.equals(sortOrder.direction()) ? "asc" : "desc";
                ArrayNode orderByArgs = innerArgs.addObject().put("ns", "op").put("fn", direction).putArray("args");
                String columnName = expressionToColumnName(sortOrder.expression());
                // This may be a bad hack to account for when the user does a groupBy/count/orderBy/limit, which does not
                // seem like the correct approach - the Spark ScanBuilder javadocs indicate that it should be limit/orderBy
                // instead. In the former scenario, we get "COUNT(*)" as the expression to order by, and we know that's not
                // the column name.
                if ("COUNT(*)".equals(columnName)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Adjusting `COUNT(*)` column to be `count`");
                    }
                    columnName = COUNT;
                }
                populateSchemaCol(orderByArgs.addObject(), columnName);
            }
        });
    }

    static ObjectNode buildSelect(StructType schema) {
        return newOperation("select", args -> {
            ArrayNode innerArgs = args.addArray();
            for (StructField field : schema.fields()) {
                populateSchemaCol(innerArgs.addObject(), field.name());
            }
        });
    }

    /**
     * Knows how to handle any of 3 variations of a column name: 1) no qualifier - "CitationID"; 2) view qualifier -
     * "myView.CitationID"; and 3) schema+view qualifier - "mySchema.myView.CitationID".
     * <p>
     * This should always be used whenever a push down operation involves constructing a column, as we need to handle
     * all 3 of the variations above. And op.schemaCol is required in order to achieve that.
     *
     * @param node
     * @param columnName
     */
    public static void populateSchemaCol(ObjectNode node, String columnName) {
        final String[] parts = removeTickMarksFromColumnName(columnName).split("\\.");

        ArrayNode colArgs = node.put("ns", "op").put("fn", "schema-col").putArray("args");
        if (parts.length == 3) {
            colArgs.add(parts[0]).add(parts[1]).add(parts[2]);
        } else if (parts.length == 2) {
            colArgs.add(objectMapper.nullNode()).add(parts[0]).add(parts[1]);
        } else {
            colArgs.add(objectMapper.nullNode()).add(objectMapper.nullNode()).add(parts[0]);
        }
    }

    /**
     * For some push down operations, the tick marks that a user must use when a column name contains a period will
     * still be present.
     *
     * @param columnName
     * @return
     */
    private static String removeTickMarksFromColumnName(String columnName) {
        if (columnName.startsWith("`")) {
            columnName = columnName.substring(1);
        }
        return columnName.endsWith("`") ?
            columnName.substring(0, columnName.length() - 1) :
            columnName;
    }

    static ObjectNode buildWhere(OpticFilter filter) {
        return newOperation("where", args -> filter.populateArg(args.addObject()));
    }

    private static ObjectNode newOperation(String name, Consumer<ArrayNode> withArgs) {
        ObjectNode operation = objectMapper.createObjectNode().put("ns", "op").put("fn", name);
        withArgs.accept(operation.putArray("args"));
        return operation;
    }

    static String expressionToColumnName(Expression expression) {
        // The structure of an Expression isn't well-understood yet. But when it refers to a single column, the
        // column name can be found in the below manner. Anything else is not supported yet.
        NamedReference[] refs = expression.references();
        if (refs == null || refs.length < 1) {
            return expression.describe();
        }
        String[] fieldNames = refs[0].fieldNames();
        if (fieldNames.length != 1) {
            throw new IllegalArgumentException("Unsupported expression: " + expression + "; expecting expression " +
                "to have exactly one field name.");
        }
        return fieldNames[0];
    }

    /**
     * Captures the name of an Optic function and the column name based on a Spark AggregateFunc's Expression. Used
     * to simplify building a serialized Optic function reference.
     */
    private static class OpticFunction {
        final String functionName;
        final String columnName;
        final boolean distinct;

        OpticFunction(String functionName, Expression column) {
            this(functionName, column, false);
        }

        OpticFunction(String functionName, Expression column, boolean distinct) {
            this.functionName = functionName;
            this.columnName = expressionToColumnName(column);
            this.distinct = distinct;
        }
    }
}
