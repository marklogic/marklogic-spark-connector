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
abstract class PlanUtil {

    private final static Logger logger = LoggerFactory.getLogger(PlanUtil.class);

    private final static ObjectMapper objectMapper = new ObjectMapper();

    static ObjectNode buildGroupByCount() {
        return newOperation("group-by", args -> args
            .add(objectMapper.nullNode())
            .addObject().put("ns", "op").put("fn", "count").putArray("args").add("count").add(objectMapper.nullNode()));
    }

    static ObjectNode buildGroupByCount(String columnName) {
        return newOperation("group-by", args -> {
            populateSchemaCol(args.addObject(), columnName);
            // Using "null" is the equivalent of "count(*)" - it counts rows, not values.
            args.addObject().put("ns", "op").put("fn", "count").putArray("args").add("count").add(objectMapper.nullNode());
        });
    }

    static ObjectNode buildLimit(int limit) {
        return newOperation("limit", args -> args.add(limit));
    }

    static ObjectNode buildOffset(int offset) {
        return newOperation("offset", args -> args.add(offset));
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

    private static void populateSchemaCol(ObjectNode node, String columnName) {
        ArrayNode colArgs = node.put("ns", "op").put("fn", "schema-col").putArray("args");
        String[] parts = columnName.split("\\.");
        if (parts.length == 3) {
            colArgs.add(parts[0]).add(parts[1]).add(parts[2]);
        } else if (parts.length == 2) {
            colArgs.add(objectMapper.nullNode()).add(parts[0]).add(parts[1]);
        } else {
            colArgs.add(objectMapper.nullNode()).add(objectMapper.nullNode()).add(parts[0]);
        }
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
