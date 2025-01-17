/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.classifier;

import com.marklogic.langchain4j.MarkLogicLangchainException;
import com.marklogic.langchain4j.Util;
import com.smartlogic.classificationserver.client.*;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.Token;
import com.smartlogic.cloud.TokenFetcher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TextClassifier {

    private static final String THRESHOLD = "20";

    private final ClassificationClient classificationClient;
    private final String conceptsArrayName;

    public TextClassifier(
        String host, String https, String port, String endpoint,
        String apikey, String tokenEndpoint, String conceptsArrayName
    ) throws CloudException {
        String protocol = "true".equalsIgnoreCase(https) ? "https" : "http";
        String tokenUrl = protocol + "://" + host + ":" + port + "/" + tokenEndpoint;

        TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, apikey);
        classificationClient = new ClassificationClient();
        Token token;
        try {
            token = tokenFetcher.getAccessToken();
        } catch (CloudException e) {
            throw new CloudException("Error retrieving token");
        }

        ClassificationConfiguration classificationConfiguration = new ClassificationConfiguration();
        classificationConfiguration.setSingleArticle(false);
        classificationConfiguration.setMultiArticle(true);
        classificationConfiguration.setSocketTimeoutMS(100000);
        classificationConfiguration.setConnectionTimeoutMS(100000);
        classificationConfiguration.setApiToken(token.getAccess_token());
        classificationConfiguration.setProtocol(protocol);
        classificationConfiguration.setHostName(host);
        classificationConfiguration.setHostPort(Integer.parseInt(port));
        classificationConfiguration.setHostPath(endpoint);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put("threshold", THRESHOLD);
        additionalParameters.put("language", "en1");
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        classificationClient.setClassificationConfiguration(classificationConfiguration);

        this.conceptsArrayName = conceptsArrayName != null ? conceptsArrayName : "concepts";
    }

    public Map<String, Collection<ClassificationScore>> classifyText(String sourceUri, String text) {
        try {
            long start = System.currentTimeMillis();
            Map<String, Collection<ClassificationScore>> results = classificationClient.getClassifiedDocument(new Body(text), new Title("")).getAllClassifications();
            if (Util.CLASSIFIER_LOGGER.isDebugEnabled()) {
                Util.CLASSIFIER_LOGGER.debug("Source URI: {}; count of results: {}; time: {}", sourceUri, results.size(),
                    (System.currentTimeMillis() - start));
            }
            return results;
        } catch (ClassificationException e) {
            throw new MarkLogicLangchainException(String.format("Unable to generate concepts for: %s; cause: %s", text, e.getMessage()), e);
        }
    }

    public String getConceptsArrayName() {
        return conceptsArrayName;
    }
}
