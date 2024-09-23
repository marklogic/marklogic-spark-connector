/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import org.apache.spark.sql.sources.*;

import com.marklogic.spark.Util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface FilterFactory {

    static OpticFilter toPlanFilter(Filter filter) {
        if (filter instanceof EqualTo) {
            EqualTo f = (EqualTo) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from EqualTo: {}", f.toString());
            }
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe f = (EqualNullSafe) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from EqualNullSafe: {}", f.toString());
            }
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from GreaterThan: {}", f.toString());
            }
            return new SingleValueFilter("gt", f.attribute(), f.value());
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from GreaterThanOrEqual: {}", f.toString());
            }
            return new SingleValueFilter("ge", f.attribute(), f.value());
        } else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from LessThan: {}", f.toString());
            }
            return new SingleValueFilter("lt", f.attribute(), f.value());
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SingleValueFilter from LessThanOrEqual: {}", f.toString());
            }
            return new SingleValueFilter("le", f.attribute(), f.value());
        } else if (filter instanceof Or) {
            Or f = (Or) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating ParentFilter from Or: {}", f.toString());
            }
            return new ParentFilter("or", f.left(), f.right());
        } else if (filter instanceof And) {
            And f = (And) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating ParentFilter from And: {}", f.toString());
            }
            return new ParentFilter("and", f.left(), f.right());
        } else if (filter instanceof Not) {
            Not f = (Not) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating ParentFilter from Not: {}", f.toString());
            }
            return new ParentFilter("not", (f.child()));
        } else if (filter instanceof IsNotNull) {
            IsNotNull f = (IsNotNull) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating IsNotNullFilter from IsNotNull: {}", f.toString());
            }
            return new IsNotNullFilter(f);
        } else if (filter instanceof IsNull) {
            IsNull f = (IsNull) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating IsNullFilter from IsNull: {}", f.toString());
            }
            return new IsNullFilter(f);
        } else if (filter instanceof In) {
            In f = (In) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating ParentFilter with SingleValueFilters from In: {}", f.toString());
            }
            return new ParentFilter("or", Stream.of(f.values())
                .map(value -> new SingleValueFilter("eq", f.attribute(), value))
                .collect(Collectors.toList()));
        } else if (filter instanceof StringContains) {
            StringContains f = (StringContains) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SqlConditionFilter from StringContains: {}", f.toString());
            }
            return new SqlConditionFilter(String.format("%s LIKE '%%%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith f = (StringStartsWith) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SqlConditionFilter from StringStartsWith: {}", f.toString());
            }
            return new SqlConditionFilter(String.format("%s LIKE '%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith f = (StringEndsWith) filter;
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Creating SqlConditionFilter from StringEndsWith: {}", f.toString());
            }
            return new SqlConditionFilter(String.format("%s LIKE '%%%s'", f.attribute(), f.value()));
        }
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Unknown filter, returning Null: {}", filter.toString());
        }
        return null;
    }
}
