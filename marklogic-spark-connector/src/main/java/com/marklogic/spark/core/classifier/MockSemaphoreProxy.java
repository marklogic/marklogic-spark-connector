/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core.classifier;

import com.marklogic.spark.dom.DOMHelper;
import org.w3c.dom.Document;

// Sonar doesn't like static assignments in this class, but this class is only used as a mock for testing.
@SuppressWarnings("java:S2696")
public class MockSemaphoreProxy implements SemaphoreProxy {

    private final Document mockResponse;
    private static int timesInvoked;
    private static boolean wasClosed;
    // Not thread-safe; tests asserting on these must use a single partition so only one proxy is created and invoked.
    private static int documentClassificationCount;
    private static String lastFileName;

    // Sonar doesn't like this static assignment, but it's fine in a class that's only used as a mock.
    @SuppressWarnings("java:S3010")
    MockSemaphoreProxy(String mockResponse) {
        this.mockResponse = new DOMHelper(null).parseXmlString(mockResponse, null);
        timesInvoked = 0;
        lastFileName = null;
        documentClassificationCount = 0;
    }

    public static boolean isClosed() {
        return wasClosed;
    }

    public static int getTimesInvoked() {
        return timesInvoked;
    }

    public static String getLastFileName() {
        return lastFileName;
    }

    public static int getDocumentClassificationCount() {
        return documentClassificationCount;
    }

    @Override
    public byte[] classifyDocument(byte[] content, String fileName, String uri) {
        lastFileName = fileName;
        documentClassificationCount++;
        String mockSingleArticleResponse = """
            <?xml version="1.0" encoding="UTF-8"?>
            <response>
              <STRUCTUREDDOCUMENT>
                <URL>../tmp/ca002056-e3f6-4c81-8c9f-00ca218330c4/1739460469_43eb</URL>
                <SYSTEM name="HASH" value="2c3bcaf41fbabf8ff2e236c7580893ec"/>
                <META name="Type" value="TEXT (4003)"/>
                <META name="title/document_title" value="/some-uri.xml"/>
                <SYSTEM name="DeterminedLanguage" value="default"/>
              </STRUCTUREDDOCUMENT>
            </response>""";

        return mockSingleArticleResponse.getBytes();
    }

    @Override
    public Document classifyArticles(byte[] multiArticleDocumentBytes) {
        timesInvoked++;
        return mockResponse;
    }

    @Override
    public void close() {
        wasClosed = true;
    }
}
