/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.sources.*;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface FilterFactory {

    static OpticFilter toPlanFilter(Filter filter) {
        return toPlanFilter(filter, null);
    }

    static OpticFilter toPlanFilter(Filter filter, Set<String> knownColumnNames) {
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
            return new ParentFilter("or", knownColumnNames, f.left(), f.right());
        } else if (filter instanceof And f) {
            return new ParentFilter("and", knownColumnNames, f.left(), f.right());
        } else if (filter instanceof Not f) {
            return new ParentFilter("not", knownColumnNames, f.child());
        } else if (filter instanceof IsNotNull f) {
            return new IsNotNullFilter(f);
        } else if (filter instanceof IsNull f) {
            return new IsNullFilter(f);
        } else if (filter instanceof In f) {
            return new ParentFilter("or", Stream.of(f.values())
                .map(value -> new SingleValueFilter("eq", f.attribute(), value))
                .collect(Collectors.toList()));
        } else if (filter instanceof StringContains f) {
            return new SqlConditionFilter(String.format("%s LIKE '%%%s%%' ESCAPE '!'",
                validateColumnName(f.attribute(), knownColumnNames), escapeLikeValue(f.value())));
        } else if (filter instanceof StringStartsWith f) {
            return new SqlConditionFilter(String.format("%s LIKE '%s%%' ESCAPE '!'",
                validateColumnName(f.attribute(), knownColumnNames), escapeLikeValue(f.value())));
        } else if (filter instanceof StringEndsWith f) {
            return new SqlConditionFilter(String.format("%s LIKE '%%%s' ESCAPE '!'",
                validateColumnName(f.attribute(), knownColumnNames), escapeLikeValue(f.value())));
        }
        return null;
    }

    private static String escapeLikeValue(String value) {
        return value.replace("!", "!!")
            .replace("%", "!%")
            .replace("_", "!_")
            .replace("'", "''");
    }

    private static String validateColumnName(String columnName, Set<String> knownColumnNames) {
        if (knownColumnNames == null || knownColumnNames.isEmpty()) {
            return columnName;
        }

        final String normalizedName = removeTickMarks(columnName);
        final boolean found = knownColumnNames.stream()
            .map(FilterFactory::removeTickMarks)
            .anyMatch(normalizedName::equals);

        if (!found) {
            throw new ConnectorException(String.format("Invalid filter column name: %s", columnName));
        }

        return columnName;
    }

    private static String removeTickMarks(String columnName) {
        if (columnName.startsWith("`")) {
            columnName = columnName.substring(1);
        }
        return columnName.endsWith("`") ?
            columnName.substring(0, columnName.length() - 1) :
            columnName;
    }
}
