/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigHelperTest {

    @Test
    void fixPaths() {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
            put(Options.WRITE_CLASSIFIER_PATH, "classifier");
            put(Options.WRITE_CLASSIFIER_TOKEN_PATH, "tokenPath");
        }};

        TextClassifierFactory.ConfigHelper helper = new TextClassifierFactory.ConfigHelper(new Context(properties));
        ClassificationConfiguration config = helper.buildClassificationConfiguration("fake-api-token");
        assertEquals("somehost", config.getHostName());
        assertEquals("/classifier", config.getHostPath(), "To ensure a valid URL, the helper should " +
            "prepend a forward slash when one does not exist.");
        assertEquals(443, config.getHostPort(), "Should default to 443");
        assertEquals("https", config.getProtocol(), "Should default to https");

        URL tokenUrl = helper.getTokenUrl();
        assertEquals("https://somehost:443/tokenPath", tokenUrl.toString(), "The token path should have a " +
            "forward slash prepended when one does not exist.");
    }

    @Test
    void defaultValues() throws MalformedURLException {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
        }};

        TextClassifierFactory.ConfigHelper helper = new TextClassifierFactory.ConfigHelper(new Context(properties));
        ClassificationConfiguration config = helper.buildClassificationConfiguration("fake-api-token");
        assertEquals("https://somehost:443/", config.getUrl());
        assertEquals("https://somehost:443/token", helper.getTokenUrl().toString());
    }

    @Test
    void overrideAllValues() throws MalformedURLException {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
            put(Options.WRITE_CLASSIFIER_PATH, "/classifier");
            put(Options.WRITE_CLASSIFIER_PORT, "8080");
            put(Options.WRITE_CLASSIFIER_TOKEN_PATH, "my-token");
            put(Options.WRITE_CLASSIFIER_HTTP, "true");
        }};

        TextClassifierFactory.ConfigHelper helper = new TextClassifierFactory.ConfigHelper(new Context(properties));
        ClassificationConfiguration config = helper.buildClassificationConfiguration("fake-api-token");
        assertEquals("http://somehost:8080/classifier", config.getUrl());
        assertEquals("http://somehost:8080/my-token", helper.getTokenUrl().toString());
    }

    @Test
    void dynamicOptions() {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
            put(Options.WRITE_CLASSIFIER_PATH, "classifier");
            put(Options.WRITE_CLASSIFIER_OPTION_PREFIX + "threshold", "17");
            put(Options.WRITE_CLASSIFIER_OPTION_PREFIX + "language", "ch1");
        }};

        TextClassifierFactory.ConfigHelper helper = new TextClassifierFactory.ConfigHelper(new Context(properties));
        ClassificationConfiguration config = helper.buildClassificationConfiguration("fake-api-token");

        Map<String, String> additionalParams = config.getAdditionalParameters();
        assertEquals("17", additionalParams.get("threshold"));
        assertEquals("ch1", additionalParams.get("language"));
    }
}
