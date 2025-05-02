/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TextClassifierFactoryTest {

    @Test
    void buildClassificationConfigurationWithoutApiKey() {
        Map<String, String> properties = new HashMap<>() {{
            put(Options.WRITE_CLASSIFIER_HOST, "somehost");
            put(Options.WRITE_CLASSIFIER_PATH, "/classifier");
            put(Options.WRITE_CLASSIFIER_PORT, "8080");
            put(Options.WRITE_CLASSIFIER_HTTP, "true");
        }};

        ClassificationConfiguration config = TextClassifierFactory.buildClassificationConfiguration(new Context(properties));
        assertNull(config.getApiToken(), "It is fine for no API key to be provided, and thus no API token will " +
            "be generated. That is suitable for on-prem Semaphore usage");
        assertEquals("somehost", config.getHostName());
        assertEquals(8080, config.getHostPort());
        assertEquals("http", config.getProtocol());
    }
}
