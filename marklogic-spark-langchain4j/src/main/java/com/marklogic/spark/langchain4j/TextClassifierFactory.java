/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.langchain4j;

import com.marklogic.langchain4j.classifier.TextClassifier;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.smartlogic.cloud.CloudException;


public interface TextClassifierFactory {

    static TextClassifier makeClassifier(Context context) {
        if (context.hasOption(Options.WRITE_CLASSIFIER_HOST)) {
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Will classify text using host: {}",
                    context.getStringOption(Options.WRITE_CLASSIFIER_HOST));
            }
            try {
                return new TextClassifier(
                    context.getStringOption(Options.WRITE_CLASSIFIER_HOST), context.getStringOption(Options.WRITE_CLASSIFIER_PROTOCOL),
                    context.getStringOption(Options.WRITE_CLASSIFIER_PORT), context.getStringOption(Options.WRITE_CLASSIFIER_ENDPOINT),
                    context.getStringOption(Options.WRITE_CLASSIFIER_APIKEY), context.getStringOption(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT)
                );
            } catch (CloudException e) {
                throw new ConnectorException(String.format("Unable to create a TextClassifier; cause: {}", e.getMessage()));
            }
        } else {
            return null;
        }
    }
}
