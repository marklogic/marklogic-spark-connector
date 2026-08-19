/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FilterFactoryTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void stringContainsEscapesSingleQuote() {
        OpticFilter filter = FilterFactory.toPlanFilter(new StringContains("LastName", "'"), Set.of("LastName"));
        assertEquals("LastName LIKE '%''%' ESCAPE '!'", extractSqlCondition(filter));
    }

    @Test
    void stringStartsWithEscapesSingleQuote() {
        OpticFilter filter = FilterFactory.toPlanFilter(new StringStartsWith("LastName", "'"), Set.of("LastName"));
        assertEquals("LastName LIKE '''%' ESCAPE '!'", extractSqlCondition(filter));
    }

    @Test
    void stringEndsWithEscapesSingleQuote() {
        OpticFilter filter = FilterFactory.toPlanFilter(new StringEndsWith("LastName", "'"), Set.of("LastName"));
        assertEquals("LastName LIKE '%''' ESCAPE '!'", extractSqlCondition(filter));
    }

    @Test
    void stringContainsEscapesLikeWildcardsAndEscapeCharacter() {
        OpticFilter filter = FilterFactory.toPlanFilter(new StringContains("LastName", "a%_!b"), Set.of("LastName"));
        assertEquals("LastName LIKE '%a!%!_!!b%' ESCAPE '!'", extractSqlCondition(filter));
    }

    @Test
    void stringContainsEscapesInjectionPayload() {
        OpticFilter filter = FilterFactory.toPlanFilter(
            new StringContains("LastName", "x%' OR 1=1 OR name LIKE '%"), Set.of("LastName"));
        assertEquals("LastName LIKE '%x!%'' OR 1=1 OR name LIKE ''!%%' ESCAPE '!'", extractSqlCondition(filter));
    }

    @Test
    void stringContainsRejectsUnknownColumn() {
        assertThrows(ConnectorException.class,
            () -> FilterFactory.toPlanFilter(new StringContains("LastName OR 1=1", "x"), Set.of("LastName")));
    }

    private String extractSqlCondition(OpticFilter filter) {
        ObjectNode arg = objectMapper.createObjectNode();
        filter.populateArg(arg);
        return arg.get("args").get(0).asText();
    }
}