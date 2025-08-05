/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.spark.reader.optic.PlanUtil;
import org.apache.spark.sql.sources.IsNotNull;

class IsNotNullFilter implements OpticFilter {

    static final long serialVersionUID = 1;

    private IsNotNull filter;

    IsNotNullFilter(IsNotNull filter) {
        this.filter = filter;
    }

    @Override
    public void populateArg(ObjectNode arg) {
        ObjectNode colArg = arg
            .put("ns", "op").put("fn", "is-defined")
            .putArray("args").addObject();

        PlanUtil.populateSchemaCol(colArg, filter.attribute());
    }

    @Override
    public PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan) {
        return plan;
    }
}
