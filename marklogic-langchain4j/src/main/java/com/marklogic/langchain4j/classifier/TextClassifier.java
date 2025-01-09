/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.classifier;

import com.marklogic.langchain4j.MarkLogicLangchainException;
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

    public TextClassifier(String host, String protocol, String port, String endpoint, String apikey, String tokenEndpoint) throws CloudException {
        String tokenUrl = protocol + "://" + host + ":" + port + "/" + tokenEndpoint;

        TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, apikey);
        classificationClient = new ClassificationClient();
        Token token;
        token = tokenFetcher.getAccessToken();

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
        additionalParameters.put("language","en1");
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        classificationClient.setClassificationConfiguration(classificationConfiguration);
    }

    public Map<String, Collection<ClassificationScore>> classifyText(String text) {
        try {
            return classificationClient.getClassifiedDocument(new Body(text), new Title("")).getAllClassifications();
        } catch (ClassificationException e) {
            throw new MarkLogicLangchainException(String.format("Unable to generate concepts for: %s; cause: %s", text, e.getMessage()), e);
        }
    }
}
