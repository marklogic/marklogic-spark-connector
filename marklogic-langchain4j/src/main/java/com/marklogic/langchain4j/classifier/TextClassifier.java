/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.classifier;

import com.marklogic.langchain4j.MarkLogicLangchainException;
import com.smartlogic.classificationserver.client.*;
import com.smartlogic.cloud.CloudException;
import com.smartlogic.cloud.Token;
import com.smartlogic.cloud.TokenFetcher;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TextClassifier {

    private static final String THRESHOLD = "20";

    private final ClassificationClient classificationClient;
    private final DocumentBuilderFactory documentBuilderFactory;

    public TextClassifier(ClassificationConfiguration classificationConfiguration) {
        documentBuilderFactory = DocumentBuilderFactory.newInstance();

        this.classificationClient = new ClassificationClient();
        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put("threshold", THRESHOLD);
        additionalParameters.put("language", "en1");
        classificationConfiguration.setAdditionalParameters(additionalParameters);
        classificationClient.setClassificationConfiguration(classificationConfiguration);
    }

    public TextClassifier(String host, String https, String port, String endpoint, String apikey, String tokenEndpoint) throws CloudException {
        String protocol = "true".equalsIgnoreCase(https) ? "https" : "http";
        String tokenUrl = protocol + "://" + host + ":" + port + "/" + tokenEndpoint;

        TokenFetcher tokenFetcher = new TokenFetcher(tokenUrl, apikey);
        classificationClient = new ClassificationClient();
        documentBuilderFactory = DocumentBuilderFactory.newInstance();
        Token token = tokenFetcher.getAccessToken();

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
    }

    public Document classifyTextToXml(String sourceUri, String text) {
        try {
            byte[] rawResponse = classificationClient.getClassificationServerResponse(new Body(text), new Title(sourceUri));
            InputSource response = new InputSource(new StringReader(new String(rawResponse, StandardCharsets.UTF_8)));
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            return builder.parse(response);
        } catch (ClassificationException | IOException | SAXException | ParserConfigurationException e) {
            throw new MarkLogicLangchainException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceUri, e.getMessage()), e);
        }
    }

    public byte[] classifyTextToBytes(String sourceUri, String text) {
        try {
            return classificationClient.getClassificationServerResponse(new Body(text), new Title(sourceUri));
        } catch (ClassificationException e) {
            throw new MarkLogicLangchainException(String.format("Unable to classify data from document with URI: %s; cause: %s", sourceUri, e.getMessage()), e);
        }
    }
}
