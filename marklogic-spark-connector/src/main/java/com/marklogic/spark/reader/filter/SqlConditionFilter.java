/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;

public class SqlConditionFilter implements OpticFilter {

    private String sqlCondition;

    public SqlConditionFilter(String sqlCondition) {
        this.sqlCondition = sqlCondition;
    }

    @Override
    public void populateArg(ObjectNode arg) {
        arg.put("ns", "op").put("fn", "sqlCondition").putArray("args").add(sqlCondition);
    }

    @Override
    public PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan) {
        return plan;
    }
}
