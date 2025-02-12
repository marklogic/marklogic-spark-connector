/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.classifier.TextClassifierFactory;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigHelperTest {

    @Test
    void fixPaths() throws Exception {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
            put(Options.WRITE_CLASSIFIER_PATH, "classifier");
            put(Options.WRITE_CLASSIFIER_TOKEN_PATH, "tokenPath");
        }};

        TextClassifierFactory.ConfigHelper helper = new TextClassifierFactory.ConfigHelper(new Context(properties));
        assertEquals("/classifier", helper.getClassifierPath(), "To ensure a valid URL, the helper should " +
            "prepend a forward slash when one does not exist.");
        assertEquals(443, helper.getPort(), "Should default to 443");
        assertEquals("https", helper.getProtocol(), "Should default to https");

        URL tokenUrl = helper.getTokenUrl();
        assertEquals("https://somehost:443/tokenPath", tokenUrl.toString(), "The token path should have a " +
            "forward slash prepended when one does not exist.");
    }
}
