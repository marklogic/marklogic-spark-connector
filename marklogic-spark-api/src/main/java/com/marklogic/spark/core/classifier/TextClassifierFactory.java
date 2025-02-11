
/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.smartlogic.classificationserver.client.ClassificationConfiguration;
import com.smartlogic.cloud.TokenFetcher;


public interface TextClassifierFactory {

    static TextClassifier newTextClassifier(Context context) {
        final String host = context.getStringOption(Options.WRITE_CLASSIFIER_HOST);
        if (host != null && host.trim().length() > 0) {
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Will classify text using host: {}",
                    context.getStringOption(Options.WRITE_CLASSIFIER_HOST));
            }
            try {
                final String protocol = context.getBooleanOption(Options.WRITE_CLASSIFIER_HTTPS, true) ? "https" : "http";
                final int port = context.getIntOption(Options.WRITE_CLASSIFIER_PORT, 443, 0);
                final String tokenEndpoint = context.getStringOption(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/");
                final String tokenUrl = String.format("%s://%s:%d/%s", protocol, host, port, tokenEndpoint);
                final String apiKey = context.getStringOption(Options.WRITE_CLASSIFIER_APIKEY);

                ClassificationConfiguration config = new ClassificationConfiguration();
                TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, apiKey);
                config.setApiToken(tokenFetcher.getAccessToken().getAccess_token());
                config.setHostName(host);
                config.setHostPort(port);
                config.setProtocol(protocol);
                config.setHostPath(context.getStringOption(Options.WRITE_CLASSIFIER_ENDPOINT));

                config.setSingleArticle(false);
                config.setMultiArticle(true);
                config.setSocketTimeoutMS(100000);
                config.setConnectionTimeoutMS(100000);
                return new TextClassifier(config);
            } catch (Exception e) {
                throw new ConnectorException(String.format("Unable to create a TextClassifier; cause: %s", e.getMessage()));
            }
        } else {
            return null;
        }
    }
}
