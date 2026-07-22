/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedactionUtilTest {

    @Test
    void findLikelySensitiveMarkLogicOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("spark.marklogic.client.password", "secret-value");
        options.put("spark.marklogic.client.connectionString", "user:password@host:8000");
        options.put("spark.marklogic.client.basePath", "/v1");
        options.put("spark.marklogic.read.opticQuery", "op.fromView('x', 'y')");
        options.put("spark.other.password", "not-marklogic");

        Set<String> found = RedactionUtil.findLikelySensitiveMarkLogicOptions(options);

        assertEquals(2, found.size());
        assertTrue(found.contains("spark.marklogic.client.password"));
        assertTrue(found.contains("spark.marklogic.client.connectionString"));
        assertFalse(found.contains("spark.marklogic.client.basePath"));
        assertFalse(found.contains("spark.marklogic.read.opticQuery"));
        assertFalse(found.contains("spark.other.password"));
    }

    @Test
    void findUnredactedSensitiveOptionsWhenRegexCoversAll() {
        Set<String> likelySensitive = Set.of(
            "spark.marklogic.client.password",
            "spark.marklogic.client.connectionString"
        );

        Set<String> uncovered = RedactionUtil.findUnredactedSensitiveOptions(
            likelySensitive,
            "(?i).*password.*|.*connectionstring.*|.*apikey.*"
        );

        assertTrue(uncovered.isEmpty());
    }

    @Test
    void findUnredactedSensitiveOptionsWhenRegexIsMissingOrInvalid() {
        Set<String> likelySensitive = Set.of(
            "spark.marklogic.client.password",
            "spark.marklogic.client.connectionString"
        );

        Set<String> uncoveredWithoutRegex = RedactionUtil.findUnredactedSensitiveOptions(likelySensitive, null);
        assertEquals(likelySensitive, uncoveredWithoutRegex);

        Set<String> uncoveredWithInvalidRegex = RedactionUtil.findUnredactedSensitiveOptions(likelySensitive, "(");
        assertEquals(likelySensitive, uncoveredWithInvalidRegex);
    }

    @Test
    void isLikelySensitiveMarkLogicOption() {
        assertTrue(RedactionUtil.isLikelySensitiveMarkLogicOption("spark.marklogic.client.password"));
        assertTrue(RedactionUtil.isLikelySensitiveMarkLogicOption("spark.marklogic.client.connectionString"));
        assertTrue(RedactionUtil.isLikelySensitiveMarkLogicOption("spark.marklogic.client.cloud.apiKey"));
        assertFalse(RedactionUtil.isLikelySensitiveMarkLogicOption("spark.marklogic.read.batchSize"));
        assertFalse(RedactionUtil.isLikelySensitiveMarkLogicOption("spark.other.password"));
    }
}
