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
