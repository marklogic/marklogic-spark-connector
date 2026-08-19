/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.dom;

import com.marklogic.spark.ConnectorException;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import static org.junit.jupiter.api.Assertions.*;

class DOMHelperTest {

    private final DOMHelper domHelper = new DOMHelper(null);

    @Test
    void validXmlIsParsedSuccessfully() {
        Document doc = domHelper.parseXmlString("<root><child>hello</child></root>", null);
        assertNotNull(doc);
        assertEquals("root", doc.getDocumentElement().getLocalName());
    }

    @Test
    void domHelperFactoryIsNamespaceAware() {
        // DOMHelper sets namespace-awareness explicitly after obtaining the secure factory.
        // Callers that do not need it (e.g. XmlChunkDocumentProducer, DocBuilder) omit it,
        // preserving their original non-namespace-aware behaviour.
        DOMHelper helper = new DOMHelper(null);
        Document doc = helper.parseXmlString("<ns:root xmlns:ns='urn:test'/>", null);
        assertNotNull(doc);
        assertEquals("root", doc.getDocumentElement().getLocalName());
    }

    @Test
    void xmlWithDoctypeIsRejected() {
        String xml = "<?xml version=\"1.0\"?><!DOCTYPE root [<!ELEMENT root (#PCDATA)>]><root>hello</root>";
        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> domHelper.parseXmlString(xml, "/test.xml"));
        assertTrue(ex.getMessage().contains("/test.xml"),
            "Error message should include the source URI; actual: " + ex.getMessage());
    }

    @Test
    void xxeFileEntityIsRejected() {
        // Attempts to read a local file via an XXE SYSTEM entity
        String xml = "<?xml version=\"1.0\"?>" +
            "<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>" +
            "<root>&xxe;</root>";
        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> domHelper.parseXmlString(xml, "/test.xml"));
        // The error must not contain the file contents — just the rejection message
        assertFalse(ex.getMessage().contains("root:"),
            "Error message must not contain file contents; actual: " + ex.getMessage());
    }

    @Test
    void billionLaughsIsRejected() {
        // Recursive entity expansion attack — would exhaust heap if not blocked
        String xml = "<?xml version=\"1.0\"?>" +
            "<!DOCTYPE lolz [" +
            "  <!ENTITY lol \"lol\">" +
            "  <!ENTITY lol2 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">" +
            "  <!ENTITY lol3 \"&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;\">" +
            "  <!ENTITY lol4 \"&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;\">" +
            "]>" +
            "<root>&lol4;</root>";
        assertThrows(ConnectorException.class,
            () -> domHelper.parseXmlString(xml, "/test.xml"));
    }
}
