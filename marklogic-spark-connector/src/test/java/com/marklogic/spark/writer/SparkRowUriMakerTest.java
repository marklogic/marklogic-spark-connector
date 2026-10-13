/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class SparkRowUriMakerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    private String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void defaultBehaviorNoHashing() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker("/employee/{id}.json", Options.WRITE_URI_TEMPLATE, false, false);
        ObjectNode row = objectMapper.createObjectNode().put("id", "12345");

        String uri = uriMaker.makeURI("/initial.json", row);
        assertEquals("/employee/12345.json", uri);
    }

    @Test
    void defaultOverloadedConstructorNoHashing() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker("/employee/{id}.json", Options.WRITE_URI_TEMPLATE, false);
        ObjectNode row = objectMapper.createObjectNode().put("id", "12345");

        String uri = uriMaker.makeURI("/initial.json", row);
        assertEquals("/employee/12345.json", uri);
    }

    @Test
    void singleExpressionHashed() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker("/employee/{id}.json", Options.WRITE_URI_TEMPLATE, false, true);
        ObjectNode row = objectMapper.createObjectNode().put("id", "12345");

        String uri = uriMaker.makeURI("/initial.json", row);
        String expectedHash = sha256Hex("12345");
        assertEquals("/employee/" + expectedHash + ".json", uri);
        assertEquals(64, expectedHash.length());
    }

    @Test
    void multipleExpressionsHashed() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/records/{dept}/{role}/{id}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ObjectNode row = objectMapper.createObjectNode()
            .put("dept", "engineering")
            .put("role", "lead")
            .put("id", "999");

        String uri = uriMaker.makeURI("/initial.json", row);
        String expectedDeptHash = sha256Hex("engineering");
        String expectedRoleHash = sha256Hex("lead");
        String expectedIdHash = sha256Hex("999");

        assertEquals(String.format("/records/%s/%s/%s.json", expectedDeptHash, expectedRoleHash, expectedIdHash), uri);
    }

    @Test
    void jsonPointerExpressionHashed() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/customers/{/customer/profile/ssn}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ObjectNode root = objectMapper.createObjectNode();
        root.putObject("customer").putObject("profile").put("ssn", "000-12-3456");

        String uri = uriMaker.makeURI("/initial.json", root);
        String expectedHash = sha256Hex("000-12-3456");
        assertEquals("/customers/" + expectedHash + ".json", uri);
    }

    @Test
    void numericAndBooleanValuesHashed() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/data/{code}/{active}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ObjectNode row = objectMapper.createObjectNode()
            .put("code", 1042)
            .put("active", true);

        String uri = uriMaker.makeURI("/initial.json", row);
        String expectedCodeHash = sha256Hex("1042");
        String expectedActiveHash = sha256Hex("true");

        assertEquals(String.format("/data/%s/%s.json", expectedCodeHash, expectedActiveHash), uri);
    }

    @Test
    void valuesWithSpecialCharactersAndSpacesHashedToSafeHex() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/special/{name}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ObjectNode row = objectMapper.createObjectNode()
            .put("name", "John Doe / Dept #5 ?");

        String uri = uriMaker.makeURI("/initial.json", row);
        String expectedHash = sha256Hex("John Doe / Dept #5 ?");

        assertEquals("/special/" + expectedHash + ".json", uri);
        assertFalse(uri.contains(" "));
        assertFalse(uri.substring("/special/".length()).contains("/"));
        assertFalse(uri.contains("?"));
        assertFalse(uri.contains("#"));
    }

    @Test
    void warnOnMissingFieldDoesNotHashUnresolvedPlaceholder() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/test/{missing}.json", Options.WRITE_URI_TEMPLATE, true, true
        );
        ObjectNode row = objectMapper.createObjectNode().put("other", "val");

        String uri = uriMaker.makeURI("/initial.json", row);
        assertTrue(uri.startsWith("/test/UNRESOLVED-"), "UNRESOLVED UUID must not be hashed. Actual: " + uri);
        assertTrue(uri.endsWith(".json"));
    }

    @Test
    void warnOnMissingFieldWithEmptyStringDoesNotHashUnresolvedPlaceholder() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/test/{empty}.json", Options.WRITE_URI_TEMPLATE, true, true
        );
        ObjectNode row = objectMapper.createObjectNode().put("empty", "   ");

        String uri = uriMaker.makeURI("/initial.json", row);
        assertTrue(uri.startsWith("/test/UNRESOLVED-"), "UNRESOLVED UUID must not be hashed when value is blank. Actual: " + uri);
        assertTrue(uri.endsWith(".json"));
    }

    @Test
    void missingFieldThrowsExceptionWhenWarnIsFalse() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/test/{missing}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ObjectNode row = objectMapper.createObjectNode().put("other", "val");

        ConnectorException ex = assertThrows(ConnectorException.class, () -> uriMaker.makeURI("/initial.json", row));
        assertTrue(ex.getMessage().contains("Expression 'missing' did not resolve to a value"));
    }

    @Test
    void nullUriTemplateValuesThrowsException() {
        SparkRowUriMaker uriMaker = new SparkRowUriMaker(
            "/test/{id}.json", Options.WRITE_URI_TEMPLATE, false, true
        );
        ConnectorException ex = assertThrows(ConnectorException.class, () -> uriMaker.makeURI("/initial.json", null));
        assertTrue(ex.getMessage().contains("no URI template values found"));
    }
}
