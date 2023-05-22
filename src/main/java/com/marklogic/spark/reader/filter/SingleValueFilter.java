package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;

import java.util.UUID;

/**
 * Can be used for any Optic operation that requires a single column name and value.
 */
class SingleValueFilter implements OpticFilter {

    final static long serialVersionUID = 1;

    private final String paramName;
    private final String functionName;
    private final String columnName;
    private final Object value;

    SingleValueFilter(String functionName, String columnName, Object value) {
        this.functionName = functionName;
        this.columnName = columnName;
        this.value = value;
        this.paramName = "FILTER_PARAM_" + UUID.randomUUID();
    }

    @Override
    public void populateArg(ObjectNode arg) {
        arg.put("ns", "op");
        arg.put("fn", this.functionName);
        ArrayNode equalArgs = arg.putArray("args");

        ObjectNode equalArg = equalArgs.addObject();
        equalArg.put("ns", "op");
        equalArg.put("fn", "col");
        equalArg.putArray("args").add(this.columnName);

        ObjectNode paramArg = equalArgs.addObject();
        paramArg.put("ns", "op");
        paramArg.put("fn", "param");
        paramArg.putArray("args").add(this.paramName);
    }

    @Override
    public PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan) {
        if (value == null) {
            return plan;
        }

        if (value instanceof Long) {
            return plan.bindParam(paramName, (Long) value);
        } else if (value instanceof Integer) {
            return plan.bindParam(paramName, (Integer) value);
        } else if (value instanceof Short) {
            // Have not found a way to test this yet, as MarkLogic returns "int" as the type in columnInfo for a TDE
            // column with a type of "short". Leaving this here in case the server reports "short" in the future.
            return plan.bindParam(paramName, (Short) value);
        } else if (value instanceof Double) {
            return plan.bindParam(paramName, (Double) value);
        } else if (value instanceof Float) {
            return plan.bindParam(paramName, (Float) value);
        } else if (value instanceof Boolean) {
            return plan.bindParam(paramName, (Boolean) value);
        } else if (value instanceof Byte) {
            // Have not found a way to test this, as MarkLogic returns "none" as the type in columnInfo for a TDE column
            // with a type of "byte". And so e.g. an IsNotNull is used instead of EqualTo. So it does not appear
            // possible to hit this line of code, but leaving it in case the server reports something besides "none"
            // in the future.
            return plan.bindParam(paramName, (Byte) value);
        }
        return plan.bindParam(paramName, value.toString());
    }
}
