/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;
import org.apache.spark.sql.sources.Filter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Supports any "parent" query that has 1 to many "child" queries. Intended to support at least "and", "or", "not", and
 * "in". This supports recursion via any depth of parent queries.
 */
class ParentFilter implements OpticFilter {

    static final long serialVersionUID = 1;

    private String functionName;
    private List<OpticFilter> filters;

    ParentFilter(String functionName, Filter... childFilters) {
        this(functionName, Stream.of(childFilters)
            .map(childFilter -> {
                OpticFilter opticFilter = FilterFactory.toPlanFilter(childFilter);
                if (opticFilter == null) {
                    throw new UnsupportedOperationException("Cannot support query; child query is not supported: " + childFilter);
                }
                return opticFilter;
            })
            .collect(Collectors.toList()));
    }

    ParentFilter(String functionName, List<OpticFilter> filters) {
        this.functionName = functionName;
        this.filters = filters;
    }

    /**
     * Per the docs at https://docs.marklogic.com/op.sqlCondition, an Optic sqlCondition returns a "filterdef"
     * instead of a boolean expression and thus cannot be used in an Optic and/or/not clause. Thus, if this filter
     * contains a SqlConditionFilter at any depth, it is not valid and cannot be pushed down.
     *
     * @return
     */
    @Override
    public boolean isValid() {
        for (OpticFilter filter : filters) {
            if (filter instanceof SqlConditionFilter) {
                return false;
            } else if (filter instanceof ParentFilter) {
                boolean valid = filter.isValid();
                if (!valid) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void populateArg(ObjectNode arg) {
        ArrayNode args = arg.put("ns", "op").put("fn", this.functionName).putArray("args");
        filters.forEach(filter -> filter.populateArg(args.addObject()));
    }

    @Override
    public PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan) {
        for (OpticFilter filter : filters) {
            plan = filter.bindFilterValue(plan);
        }
        return plan;
    }
}
