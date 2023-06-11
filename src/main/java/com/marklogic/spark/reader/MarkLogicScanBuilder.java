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
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.Avg;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class MarkLogicScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit,
    SupportsPushDownTopN, SupportsPushDownAggregates, SupportsPushDownRequiredColumns {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicScanBuilder.class);

    private ReadContext readContext;
    private List<Filter> pushedFilters;

    private final static Set<Class> SUPPORTED_AGGREGATE_FUNCTIONS = new HashSet() {{
        add(Avg.class);
        add(Count.class);
        add(CountStar.class);
        add(Max.class);
        add(Min.class);
        add(Sum.class);
    }};

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
                if (logger.isInfoEnabled()) {
                    logger.info("Pushing down filter: {}", filter);
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
        if (logger.isInfoEnabled()) {
            logger.info("Pushing down limit: {}", limit);
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
        if (logger.isInfoEnabled()) {
            logger.info("Pushing down topN: {}; limit: {}", Arrays.asList(orders), limit);
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

    /**
     * Per the Spark javadocs, this should return true if we can push down the entire aggregation. This is only
     * possible if every aggregation function is supported and if only one request will be made to MarkLogic. If
     * multiple requests are made to MarkLogic (based on the user-defined partition count and batch size), then
     * Spark has to apply the aggregation against the combined set of rows returned from all requests to MarkLogic.
     *
     * @param aggregation
     * @return
     */
    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        if (readContext.planAnalysisFoundNoRows()) {
            return false;
        }

        if (hasUnsupportedAggregateFunction(aggregation)) {
            logger.info("Aggregation contains one or more unsupported functions, " +
                "so not pushing aggregation to MarkLogic: {}", describeAggregation(aggregation));
            return false;
        }

        if (readContext.getBucketCount() > 1) {
            logger.info("Multiple requests will be made to MarkLogic; aggregation will be applied by Spark as well: {}",
                describeAggregation(aggregation));
            return false;
        }
        return true;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        // For the initial 2.0 release, there aren't any known unsupported aggregate functions that can be called
        // after a "groupBy". If one is detected though, the aggregation won't be pushed down as it's uncertain if
        // pushing it down would produce the correct results.
        if (readContext.planAnalysisFoundNoRows() || hasUnsupportedAggregateFunction(aggregation)) {
            return false;
        }
        logger.info("Pushing down aggregation: {}", describeAggregation(aggregation));
        readContext.pushDownAggregation(aggregation);
        return true;
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
            // Keeping this at debug level as it can be fairly verbose.
            if (logger.isDebugEnabled()) {
                logger.debug("Pushing down required schema: {}", requiredSchema.json());
            }
            readContext.pushDownRequiredSchema(requiredSchema);
        }
    }

    private boolean hasUnsupportedAggregateFunction(Aggregation aggregation) {
        return Stream
            .of(aggregation.aggregateExpressions())
            .anyMatch(func -> !SUPPORTED_AGGREGATE_FUNCTIONS.contains(func.getClass()));
    }

    private String describeAggregation(Aggregation aggregation) {
        return String.format("groupBy: %s; aggregates: %s",
            Arrays.asList(aggregation.groupByExpressions()),
            Arrays.asList(aggregation.aggregateExpressions()));
    }
}
