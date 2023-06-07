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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.reader.filter.OpticFilter;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * Methods for modifying a serialized Optic plan. These were moved here both to facilitate unit testing for some of them
 * and to simplify {@code ReadContext}.
 */
public abstract class PlanUtil {

    private final static Logger logger = LoggerFactory.getLogger(PlanUtil.class);

    private final static ObjectMapper objectMapper = new ObjectMapper();

    static ObjectNode buildGroupByCount() {
        return newOperation("group-by", args -> {
            args.add(objectMapper.nullNode());
            addCountArg(args);
        });
    }

    static ObjectNode buildGroupByCount(String columnName) {
        return newOperation("group-by", args -> {
            populateSchemaCol(args.addObject(), columnName);
            addCountArg(args);
        });
    }

    private static void addCountArg(ArrayNode args) {
        args.addObject().put("ns", "op").put("fn", "count").putArray("args")
            // "count" is used as the column name as that's what Spark uses when the operation is not pushed down.
            .add("count")
            // Using "null" is the equivalent of "count(*)" - it counts rows, not values.
            .add(objectMapper.nullNode());
    }

    static ObjectNode buildLimit(int limit) {
        return newOperation("limit", args -> args.add(limit));
    }

    static ObjectNode buildOrderBy(SortOrder sortOrder) {
        final String direction = SortDirection.ASCENDING.equals(sortOrder.direction()) ? "asc" : "desc";
        final String columnName = expressionToColumnName(sortOrder.expression());
        return newOperation("order-by", args -> {
            ArrayNode orderByArgs = args.addObject().put("ns", "op").put("fn", direction).putArray("args");
            // This may be a bad hack to account for when the user does a groupBy/count/orderBy/limit, which does not
            // seem like the correct approach - the Spark ScanBuilder javadocs indicate that it should be limit/orderBy
            // instead. In the former scenario, we get "COUNT(*)" as the expression to order by, and we know that's not
            // the column name.
            if (logger.isDebugEnabled()) {
                logger.debug("Adjusting `COUNT(*)` column to be `count`");
            }
            populateSchemaCol(orderByArgs.addObject(), "COUNT(*)".equals(columnName) ? "count" : columnName);
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

    static ObjectNode buildWhere(List<OpticFilter> opticFilters) {
        return newOperation("where", args -> {
            // If there's only one filter, can toss it into the "where" clause. Else, toss an "and" into the "where" and
            // then toss every filter into the "and" clause (which accepts 2 to N args).
            final ArrayNode targetArgs = opticFilters.size() == 1 ?
                args :
                args.addObject().put("ns", "op").put("fn", "and").putArray("args");

            opticFilters.forEach(planFilter -> planFilter.populateArg(targetArgs.addObject()));
        });
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
}
