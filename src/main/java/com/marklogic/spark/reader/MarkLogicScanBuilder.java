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
package com.marklogic.spark.reader;

import com.marklogic.spark.reader.filter.FilterFactory;
import com.marklogic.spark.reader.filter.OpticFilter;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MarkLogicScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit,
    SupportsPushDownOffset, SupportsPushDownTopN, SupportsPushDownAggregates, SupportsPushDownRequiredColumns {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicScanBuilder.class);

    private ReadContext readContext;
    private List<Filter> pushedFilters;

    public MarkLogicScanBuilder(ReadContext readContext) {
        this.readContext = readContext;
    }

    @Override
    public Scan build() {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating new scan");
        }
        return new MarkLogicScan(readContext);
    }

    /**
     * @param filters
     * @return an array of filters that are not pushed down so that Spark knows it must apply them to data returned
     * by the connector
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        pushedFilters = new ArrayList<>();
        if (readContext.planAnalysisFoundNoRows()) {
            return filters;
        }

        List<Filter> unsupportedFilters = new ArrayList<>();
        List<OpticFilter> opticFilters = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("Filter count: {}", filters.length);
        }
        for (Filter filter : filters) {
            OpticFilter opticFilter = FilterFactory.toPlanFilter(filter);
            if (opticFilter != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Pushing down filter: {}", filter);
                }
                opticFilters.add(opticFilter);
                this.pushedFilters.add(filter);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Unsupported filter, will be handled by Spark: {}", filter);
                }
                unsupportedFilters.add(filter);
            }
        }

        readContext.pushDownFiltersIntoOpticQuery(opticFilters);
        return unsupportedFilters.toArray(new Filter[0]);
    }

    /**
     * @return array of filters that were pushed down to MarkLogic so that Spark knows not to apply them itself
     */
    @Override
    public Filter[] pushedFilters() {
        return pushedFilters.toArray(new Filter[0]);
    }

    @Override
    public boolean pushLimit(int limit) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Pushing down limit: {}", limit);
        }
        readContext.pushDownLimit(limit);
        return true;
    }

    @Override
    public boolean pushTopN(SortOrder[] orders, int limit) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }
        // This will be invoked when the user calls both orderBy and limit in their Spark program. If the user only
        // calls limit, then only pushLimit is called and this will not be called. If the user only calls orderBy and
        // not limit, then neither this nor pushLimit will be called.
        if (logger.isDebugEnabled()) {
            logger.debug("Pushing down topN: {}; limit: {}", Arrays.asList(orders), limit);
        }
        readContext.pushDownTopN(orders, limit);
        return true;
    }

    @Override
    public boolean isPartiallyPushed() {
        // If a single bucket exists - i.e. a single call will be made to MarkLogic - then any limit/orderBy can be
        // fully pushed down to MarkLogic. Otherwise, we also need Spark to apply the limit/orderBy to the returned rows
        // to ensure that the user receives the correct response.
        return readContext.getBucketCount() > 1;
    }

    @Override
    public boolean pushOffset(int offset) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Pushing down offset: {}", offset);
        }
        readContext.pushDownOffset(offset);
        return true;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }
        if (supportCompletePushDown(aggregation)) {
            if (aggregation.groupByExpressions().length > 0) {
                Expression expr = aggregation.groupByExpressions()[0];
                if (logger.isDebugEnabled()) {
                    logger.debug("Pushing down by groupBy + count on: {}", expr.describe());
                }
                readContext.pushDownGroupByCount(expr);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Pushing down count()");
                }
                readContext.pushDownCount();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }
        AggregateFunc[] expressions = aggregation.aggregateExpressions();
        if (expressions.length == 1 && expressions[0] instanceof CountStar) {
            // If a count() is used, it's supported if there's no groupBy - i.e. just doing a count() by itself -
            // and supported with a single groupBy - e.g. groupBy("column").count().
            return aggregation.groupByExpressions().length < 2;
        }
        return false;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        if (readContext.planAnalysisFoundNoRows()) {
            return;
        }
        if (requiredSchema.equals(readContext.getSchema())) {
            if (logger.isDebugEnabled()) {
                logger.debug("The schema to push down is equal to the existing schema, so not pushing it down.");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Pushing down required schema: {}", requiredSchema.json());
            }
            readContext.pushDownRequiredSchema(requiredSchema);
        }
    }
}
