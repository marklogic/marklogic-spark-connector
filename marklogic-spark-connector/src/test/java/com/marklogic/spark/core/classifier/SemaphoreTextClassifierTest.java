/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.core.DocumentInputs;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SemaphoreTextClassifierTest {

    @Test
    void classifyDocumentPassesDerivedFileNameAndOriginalUriToProxy() {
        AtomicReference<String> capturedFileName = new AtomicReference<>();
        AtomicReference<String> capturedUri = new AtomicReference<>();
        byte[] expectedClassification = "<response/>".getBytes(StandardCharsets.UTF_8);

        SemaphoreProxy capturingProxy = new SemaphoreProxy() {
            @Override
            public byte[] classifyDocument(byte[] content, String fileName, String uri) {
                capturedFileName.set(fileName);
                capturedUri.set(uri);
                return expectedClassification;
            }

            @Override
            public Document classifyArticles(byte[] multiArticleDocumentBytes) {
                return null;
            }

            @Override
            public void close() {
            }
        };

        String longUri = "/deep/path/" + "x".repeat(150) + ".json";
        DocumentInputs inputs = new DocumentInputs(
            longUri,
            new StringHandle("{\"hello\":\"world\"}").withFormat(Format.JSON),
            null,
            null
        );

        SemaphoreTextClassifier classifier = new SemaphoreTextClassifier(capturingProxy, "UTF-8", 20);
        classifier.classifyDocument(inputs);

        assertEquals("x".repeat(95) + ".json", capturedFileName.get());
        assertEquals(longUri, capturedUri.get());
        assertArrayEquals(expectedClassification, inputs.getDocumentClassification());
    }
}
