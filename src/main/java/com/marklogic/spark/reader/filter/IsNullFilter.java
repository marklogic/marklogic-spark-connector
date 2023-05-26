package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.spark.reader.PlanUtil;
import org.apache.spark.sql.sources.IsNull;

class IsNullFilter implements OpticFilter {

    final static long serialVersionUID = 1;

    private IsNull filter;

    IsNullFilter(IsNull filter) {
        this.filter = filter;
    }

    @Override
    public void populateArg(ObjectNode arg) {
        ObjectNode colArg = arg
            .put("ns", "op").put("fn", "not")
            .putArray("args").addObject()
            .put("ns", "op").put("fn", "is-defined")
            .putArray("args").addObject();

        PlanUtil.populateSchemaCol(colArg, filter.attribute());
    }

    @Override
    public PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan) {
        return plan;
    }
}
