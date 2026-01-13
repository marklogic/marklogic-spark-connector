/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */

/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;

import java.util.Objects;
import java.util.function.Supplier;

public abstract class TextClassifierFactory {

    private static final String MOCK_CLASSIFIER_OPTION = "spark.marklogic.testing.mockClassifierResponse";

    public static TextClassifier newTextClassifier(Context context) {
        SemaphoreProxy semaphoreProxy = null;
        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);

        if (context.hasOption(MOCK_CLASSIFIER_OPTION)) {
            String mockResponse = context.getStringOption(MOCK_CLASSIFIER_OPTION);
            Objects.requireNonNull(mockResponse);
            semaphoreProxy = new MockSemaphoreProxy(mockResponse);
        } else if (host != null && !host.trim().isEmpty()) {
            try {
                final ConfigHelper configHelper = new ConfigHelper(context);
                ClassificationConfiguration config = configHelper.buildClassificationConfiguration();

                final String apiKey = context.getStringOption(Options.WRITE_CLASSIFIER_APIKEY);
                Supplier<String> tokenGenerator = apiKey != null && !apiKey.trim().isEmpty() ?
                    new TokenGenerator(configHelper, apiKey) : null;

                semaphoreProxy = new SemaphoreProxyImpl(config, tokenGenerator);
            } catch (ConnectorException ex) {
                throw ex;
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to configure a connection for classifying text; cause: %s",
                    e.getMessage()), e);
            }
        }

        if (semaphoreProxy != null) {
            // We may need a dedicated encoding for this
            String encoding = context.getStringOption(Options.READ_FILES_ENCODING, "UTF-8");
            int batchSize = context.getIntOption(Options.WRITE_CLASSIFIER_BATCH_SIZE, 20, 1);
            return new SemaphoreTextClassifier(semaphoreProxy, encoding, batchSize);
        }
        return null;
    }

    private TextClassifierFactory() {
    }
}
