/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.writer.DocumentProcessor;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test verifies the SplitterDocumentProcessor without involving Spark at all. The intent is to ensure that all
 * the splitter plumbing works correctly, while other test will verify that all the splitter-specific Spark options
 * produce the expected result.
 */
class SplitterTest extends AbstractIntegrationTest {

    @Test
    void textPath() {
        XmlNode doc = splitTestDocument("/root/text/text()");

        doc.assertElementCount(
            "Expecting the default splitter to split the 'text' element into 4 chunks, each having its own 'text' element.",
            "/root/chunks/chunk[text/text()]", 4);
    }

    @Test
    void elementPath() {
        XmlNode doc = splitTestDocument("/root/nested");

        doc.assertElementCount("Only expecting one chunk since the root/nested/text element has very little text",
            "/root/chunks/chunk", 1);

        String value = doc.getElementValue("/root/chunks/chunk/text");
        assertTrue(value.startsWith("<nested>"), "When a user selects an element, the expectation is that the " +
            "entire XML fragment is returned and sent to the splitter. For our default langchain4j splitter, this " +
            "isn't useful because it doesn't know to do anything useful with XML tags. But this verifies that the " +
            "splitter did receive the XML fragment, which is then set as the value of the 'text' element " +
            "in the only chunk. Actual value: " + value);
    }

    @Test
    void attributePath() {
        XmlNode doc = splitTestDocument("/root/attribute-test/@text");
        doc.assertElementCount("/root/chunks/chunk", 1);
        doc.assertElementValue("It should be rare that a user wants to split the text in an attribute, but it should " +
                "be feasible. We don't have a way though of preserving the attribute name in some sort of serialization " +
                "with JDOM2; we can only get the attribute value.",
            "/root/chunks/chunk/text",
            "Some attribute text."
        );
    }

    @Test
    void wholeDocument() {
        XmlNode doc = splitTestDocument("/");

        doc.assertElementCount(
            "When the user selects the entire document, it should be serialized into a string that is passed to the " +
                "splitter. And the default splitter will turn that into 6 chunks.",
            "/root/chunks/chunk", 6);

        String firstChunk = doc.getElementValue("/root/chunks/chunk[1]/text");
        assertTrue(firstChunk.startsWith("<?xml version="), "The first chunk is expected to contain the " +
            "start of the serialized document, which will begin with the XML declaration. Actual chunk: " + firstChunk);
    }

    @Test
    void multipleMatchingElements() {
        XmlNode doc = splitTestDocument("//node()[local-name(.) = 'url' or (local-name(.) = 'text' and ancestor::nested)]/text()");

        doc.assertElementCount(
            "Should have text from 2 elements, but that's small enough for 1 chunk",
            "/root/chunks/chunk", 1);

        doc.assertElementValue(
            "The single chunk should have the concatenation of the two selected elements, joined with a space.",
            "/root/chunks/chunk/text", "https://docs.marklogic.com/guide/java/intro This is for testing.");
    }

    @Test
    void noMatches() {
        XmlNode doc = splitTestDocument("/doesnt/match/anything");
        doc.assertElementMissing("When no text is selected, no chunks should be added.", "/root/chunks");
    }

    private XmlNode splitTestDocument(String xpath) {
        DocumentWriteOperation sourceDocument = readXmlDocument();
        DocumentWriteOperation output = newXmlSplitter(xpath).apply(sourceDocument).next();
        String xml = HandleAccessor.contentAsString(output.getContent());
        return new XmlNode(xml);
    }

    private DocumentProcessor newXmlSplitter(String path) {
        return new SplitterDocumentProcessor(
            new JDOMTextSelector(path, null),
            DocumentSplitters.recursive(500, 0),
            new DefaultChunkAssembler()
        );
    }

    private DocumentWriteOperation readXmlDocument() {
        final String uri = "/marklogic-docs/java-client-intro.xml";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        StringHandle contentHandle = getDatabaseClient().newXMLDocumentManager().read(uri, metadata, new StringHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }
}
