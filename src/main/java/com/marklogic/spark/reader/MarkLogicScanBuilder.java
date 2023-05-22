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
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MarkLogicScanBuilder implements ScanBuilder, SupportsPushDownFilters {

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
        List<Filter> unsupportedFilters = new ArrayList<>();
        List<OpticFilter> opticFilters = new ArrayList<>();
        pushedFilters = new ArrayList<>();
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
}
