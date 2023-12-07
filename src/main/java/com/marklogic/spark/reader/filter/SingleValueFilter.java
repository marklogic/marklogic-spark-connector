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
package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.spark.reader.PlanUtil;

import java.util.UUID;

/**
 * Can be used for any Optic operation that requires a single column name and value.
 */
class SingleValueFilter implements OpticFilter {

    static final long serialVersionUID = 1;

    private final String paramName;
    private final String functionName;
    private final String columnName;

    // This warning about the value not being serializable is ignored, as we trust Spark to only ever have
    // serializable values in its filters.
    @SuppressWarnings("java:S1948")
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
        ArrayNode functionArgs = arg.putArray("args");

        PlanUtil.populateSchemaCol(functionArgs.addObject(), this.columnName);

        ObjectNode paramArg = functionArgs.addObject();
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
