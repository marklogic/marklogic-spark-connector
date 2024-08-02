/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import org.apache.spark.sql.sources.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface FilterFactory {

    static OpticFilter toPlanFilter(Filter filter) {
        if (filter instanceof EqualTo) {
            EqualTo f = (EqualTo) filter;
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe f = (EqualNullSafe) filter;
            return new SingleValueFilter("eq", f.attribute(), f.value());
        } else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            return new SingleValueFilter("gt", f.attribute(), f.value());
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            return new SingleValueFilter("ge", f.attribute(), f.value());
        } else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            return new SingleValueFilter("lt", f.attribute(), f.value());
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            return new SingleValueFilter("le", f.attribute(), f.value());
        } else if (filter instanceof Or) {
            Or f = (Or) filter;
            return new ParentFilter("or", f.left(), f.right());
        } else if (filter instanceof And) {
            And f = (And) filter;
            return new ParentFilter("and", f.left(), f.right());
        } else if (filter instanceof Not) {
            return new ParentFilter("not", (((Not) filter).child()));
        } else if (filter instanceof IsNotNull) {
            return new IsNotNullFilter((IsNotNull) filter);
        } else if (filter instanceof IsNull) {
            return new IsNullFilter((IsNull) filter);
        } else if (filter instanceof In) {
            In f = (In) filter;
            return new ParentFilter("or", Stream.of(f.values())
                .map(value -> new SingleValueFilter("eq", f.attribute(), value))
                .collect(Collectors.toList()));
        } else if (filter instanceof StringContains) {
            StringContains f = (StringContains) filter;
            return new SqlConditionFilter(String.format("%s LIKE '%%%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith f = (StringStartsWith) filter;
            return new SqlConditionFilter(String.format("%s LIKE '%s%%'", f.attribute(), f.value()));
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith f = (StringEndsWith) filter;
            return new SqlConditionFilter(String.format("%s LIKE '%%%s'", f.attribute(), f.value()));
        }
        return null;
    }
}
