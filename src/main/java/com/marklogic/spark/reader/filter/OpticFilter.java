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

import java.io.Serializable;

/**
 * Intended to encapsulate how each supported Spark filter can be converted into an equivalent Optic function for
 * filtering rows.
 */
public interface OpticFilter extends Serializable {

    /**
     * @param arg an object passed to the "args" array of either an op.where or op.and call; implementor should
     *            populate this with its details - e.g. ns, fn, and its own args.
     */
    void populateArg(ObjectNode arg);

    /**
     * Each implementation of this interface is expected to use op.param for any values that it needs to bind
     * to allow for the server to cache the query. This provides a chance for the implementation to bind values. If
     * an implementation does not use op.param, it can simply return the plan as-is.
     *
     * @param plan
     * @return
     */
    PlanBuilder.Plan bindFilterValue(PlanBuilder.Plan plan);
}
