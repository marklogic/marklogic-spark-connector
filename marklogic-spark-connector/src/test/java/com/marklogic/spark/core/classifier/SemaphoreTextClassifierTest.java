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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

class SemaphoreTextClassifierTest {

    @Test
    void classifyDocumentPassesDerivedFileNameAndOriginalUriToProxy() {
        AtomicReference<String> capturedFileName = new AtomicReference<>();
        AtomicReference<String> capturedUri = new AtomicReference<>();
        byte[] expectedClassification = "<response/>".getBytes(StandardCharsets.UTF_8);

        SemaphoreProxy capturingProxy = new TestProxy() {
            @Override
            public byte[] classifyDocument(byte[] content, String fileName, String uri) {
                capturedFileName.set(fileName);
                capturedUri.set(uri);
                return expectedClassification;
            }
        };

        String longUri = "/deep/path/" + "x".repeat(150) + ".json";
        DocumentInputs inputs = newJsonInputs(longUri);

        new SemaphoreTextClassifier(capturingProxy, "UTF-8", 20, false).classifyDocument(inputs);

        assertEquals("x".repeat(95) + ".json", capturedFileName.get());
        assertEquals(longUri, capturedUri.get());
        assertArrayEquals(expectedClassification, inputs.getDocumentClassification());
    }

    @Test
    void skipDocumentDoesNotInvokeProxyAndLeavesDocumentClassificationNull() {
        SemaphoreProxy failingProxy = new TestProxy() {
            @Override
            public byte[] classifyDocument(byte[] content, String fileName, String uri) {
                return fail("classifyDocument should not be invoked when skipDocument is true");
            }
        };

        DocumentInputs inputs = newJsonInputs("/some/doc.json");

        new SemaphoreTextClassifier(failingProxy, "UTF-8", 20, true).classifyDocument(inputs);

        assertNull(inputs.getDocumentClassification());
    }

    private static DocumentInputs newJsonInputs(String uri) {
        return new DocumentInputs(uri, new StringHandle("{\"hello\":\"world\"}").withFormat(Format.JSON), null, null);
    }

    private abstract static class TestProxy implements SemaphoreProxy {
        @Override
        public Document classifyArticles(byte[] multiArticleDocumentBytes) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
