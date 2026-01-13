/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import org.apache.spark.sql.sources.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface FilterFactory {

    static OpticFilter toPlanFilter(Filter filter) {
        if (filter instanceof EqualTo f) {
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof EqualNullSafe f) {
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof GreaterThan f) {
            return new SingleValueFilter("gt", f.attribute(), f.value());
        } else if (filter instanceof GreaterThanOrEqual f) {
            return new SingleValueFilter("ge", f.attribute(), f.value());
        } else if (filter instanceof LessThan f) {
            return new SingleValueFilter("lt", f.attribute(), f.value());
        } else if (filter instanceof LessThanOrEqual f) {
            return new SingleValueFilter("le", f.attribute(), f.value());
        } else if (filter instanceof Or f) {
            return new ParentFilter("or", f.left(), f.right());
        } else if (filter instanceof And f) {
            return new ParentFilter("and", f.left(), f.right());
        } else if (filter instanceof Not f) {
            return new ParentFilter("not", f.child());
        } else if (filter instanceof IsNotNull f) {
            return new IsNotNullFilter(f);
        } else if (filter instanceof IsNull f) {
            return new IsNullFilter(f);
        } else if (filter instanceof In f) {
            return new ParentFilter("or", Stream.of(f.values())
                .map(value -> new SingleValueFilter("eq", f.attribute(), value))
                .collect(Collectors.toList()));
        } else if (filter instanceof StringContains f) {
            return new SqlConditionFilter(String.format("%s LIKE '%%%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringStartsWith f) {
            return new SqlConditionFilter(String.format("%s LIKE '%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringEndsWith f) {
            return new SqlConditionFilter(String.format("%s LIKE '%%%s'", f.attribute(), f.value()));
        }
        return null;
    }
}
