/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileUtilTest {

    @Test
    void makePathFromRegularURI() {
        String uri = FileUtil.makePathFromDocumentURI("/path/to/doc.xml");
        assertEquals("/path/to/doc.xml", uri);
    }

    @Test
    void makePathFromOpaqueURI() {
        String uri = FileUtil.makePathFromDocumentURI("org:example:123.xml");
        assertEquals("example:123.xml", uri);
    }

    @Test
    void makePathWithInvalidURI() {
        String uri = FileUtil.makePathFromDocumentURI("has space.json");
        assertEquals("has space.json", uri, "If a java.net.URI cannot be constructed - in this case, it's due to " +
            "the space in the string - then the error should be logged at the DEBUG level and the original value " +
            "should be returned.");
    }
}
