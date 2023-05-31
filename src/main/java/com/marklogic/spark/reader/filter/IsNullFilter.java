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
