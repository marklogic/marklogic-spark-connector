/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.Token;
import com.smartlogic.cloud.TokenFetcher;

import java.util.Objects;
import java.util.function.Supplier;

// Handles generating an access token using the Semaphore cloud client.
record TokenGenerator(ConfigHelper configHelper, String apiKey) implements Supplier<String> {

    @Override
    public String get() {
        TokenFetcher tokenFetcher = new TokenFetcher(configHelper.getTokenUrl().toString(), apiKey);
        try {
            Token token = tokenFetcher.getAccessToken();
            Objects.requireNonNull(token);
            return token.getAccess_token();
        } catch (CloudException e) {
            throw new ConnectorException(String.format("Unable to generate token for classifying text; cause: %s", e.getMessage()), e);
        }
    }
}
