/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.langchain4j;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MakeEmbeddingModelTest {

    private static final String VALID_FUNCTION_CLASS =
        "com.marklogic.langchain4j.StubEmbeddingModelFunction";

    /**
     * AC3: A class that exists but does NOT implement Function must cause a ConnectorException to be thrown
     * immediately, before getDeclaredConstructor().newInstance() is ever called.
     * java.lang.String is used as a convenient existing class that does not implement Function.
     */
    @Test
    void classExistsButDoesNotImplementFunction() {
        Context context = contextWithOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "java.lang.String");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> Langchain4jFactory.makeEmbeddingModel(context));

        String message = ex.getMessage();
        assertTrue(message.contains("java.lang.String"),
            "Error message should contain the class name; was: " + message);
        assertTrue(message.contains("java.util.function.Function"),
            "Error message should name the required interface; was: " + message);
        assertTrue(message.contains(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME),
            "Error message should name the option; was: " + message);
        // Must NOT be wrapped in the generic 'Unable to instantiate' message
        assertFalse(message.startsWith("Unable to instantiate class"),
            "ConnectorException should be thrown directly, not wrapped; was: " + message);
    }

    /**
     * AC4: A valid Function class must be resolved and return a non-empty Optional without errors.
     */
    @Test
    void validFunctionClassSucceeds() {
        Context context = contextWithOption(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, VALID_FUNCTION_CLASS);

        Optional<EmbeddingModel> result = Langchain4jFactory.makeEmbeddingModel(context);

        assertTrue(result.isPresent(), "A valid function class should produce an EmbeddingModel");
    }

    private static Context contextWithOption(String key, String value) {
        Map<String, String> options = new HashMap<>();
        options.put(key, value);
        return new Context(options);
    }
}
