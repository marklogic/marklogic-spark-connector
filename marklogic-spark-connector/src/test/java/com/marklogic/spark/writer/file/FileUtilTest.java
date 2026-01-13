/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileUtilTest {

    @Test
    void makePathFromRegularURI() {
        assertEquals("/base/path/to/doc.xml", makePath("/path/to/doc.xml"));
    }

    @Test
    void noForwardSlash() {
        assertEquals("/base/path/to/doc.xml", makePath("path/to/doc.xml"));
    }

    @Test
    void justFilename() {
        assertEquals("/base/doc.xml", makePath("doc.xml"));
    }

    /**
     * This was altered in the 2.7.0 to fix a bug where a URI with two or more colons and
     * no leading slash causing a URISyntaxException when the Hadoop Path constructor was called. This
     * test was only testing the construction of the String path and not the Hadoop Path, which is how
     * the bugged was missed. Based on Copilot's advice of prepending "./" to an opaque URI to make the Hadoop Path
     * constructor happy, we no longer construct a java.net.URI and no longer call "getSchemeSpecificPart", which
     * was dropping the text occurring before the first colon.
     */
    @Test
    void opaqueURI() {
        assertEquals("/base/org:example:123.xml", makePath("org:example:123.xml"),
            "An opaque URI - i.e. with colons and no leading '/' should be prefixed with './' to make it a valid " +
                "Hadoop Path.");
    }

    @Test
    void uriWithSpace() {
        assertEquals("/base/has space.json", makePath("has space.json"));
    }

    @Test
    void allKindsOfStuff() {
        assertEquals("/base/has+lots of&/stuff_in-it.json", makePath("has+lots of&/stuff_in-it.json"));
    }

    @Test
    void fileBasedUri() {
        assertEquals("/base/hey.json", makePath("file://tmp/hey.json"), "Per the fixes for 1.4.0, " +
            "we're retaining the behavior where the scheme is removed for a non-opaque URI. This ensures we don't " +
            "try to files with e.g. 'http://' or 'file://' in the URI. This won't prevent issues like writing files " +
            "to Azure Storage, which doesn't allow colons - a colon can appear anywhere in the URI and still be valid.");
    }

    @Test
    void httpBasedUri() {
        assertEquals("/base/path/to/file.xml", makePath("https://www.exampple.org/path/to/file.xml"));
    }

    private String makePath(String uri) {
        return FileUtil.makePathFromDocumentURI("/base", uri).toString();
    }
}
