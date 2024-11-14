/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This test verifies the SplitterDocumentProcessor without involving Spark at all. The intent is to ensure that all
 * the splitter plumbing works correctly, while other test will verify that all the splitter-specific Spark options
 * produce the expected result.
 */
class SplitterTest extends AbstractIntegrationTest {

    @Test
    void textPath() {
        XmlNode doc = splitTextDocument("/root/text/text()");

        doc.assertElementCount(
            "Expecting the default splitter to split the 'text' element into 4 chunks, each having its own 'text' element.",
            "/root/chunks/chunk[text/text()]", 4);
    }

    @Test
    void elementPath() {
        XmlNode doc = splitTextDocument("/root/nested");
        doc.assertElementCount("Only expecting one chunk since the root/nested/text element has very little text",
            "/root/chunks/chunk", 1);

        String value = doc.getElementValue("/root/chunks/chunk/text");
        assertEquals("This is for testing.", value, "With our DOM-based implementation, we can easily return the " +
            "text content of any node selected by the user's path. We may eventually support an option to instead " +
            "serialize the selected node into a string.");
    }

    @Test
    void attributePath() {
        XmlNode doc = splitTextDocument("/root/attribute-test/@text");
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
        XmlNode doc = splitTextDocument("/");
        doc.assertElementMissing("With the DOM library, the user needs to select something - '/' does not have any " +
            "text content, so no text is selected, and thus no chunks are created.", "/root/chunks");
    }

    @Test
    void multipleMatchingElements() {
        XmlNode doc = splitTextDocument("//node()[local-name(.) = 'url' or (local-name(.) = 'text' and ancestor::nested)]/text()");

        doc.assertElementCount(
            "Should have text from 2 elements, but that's small enough for 1 chunk",
            "/root/chunks/chunk", 1);

        doc.assertElementValue(
            "The single chunk should have the concatenation of the two selected elements, joined with a space.",
            "/root/chunks/chunk/text", "https://docs.marklogic.com/guide/java/intro This is for testing.");
    }

    @Test
    void noMatches() {
        XmlNode doc = splitTextDocument("/doesnt/match/anything");
        doc.assertElementMissing("When no text is selected, no chunks should be added.", "/root/chunks");
    }

    @Test
    void json() {
        JsonNode doc = splitJsonDocument("/text");
        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        assertEquals(4, chunks.size(), "Expecting 4 chunks based on a max chunk size of 500.");
        JsonNode firstChunk = chunks.get(0);
        assertTrue(firstChunk.has("text"));
        String value = firstChunk.get("text").asText();
        assertTrue(value.startsWith("When working with the Java API"), "Unexpected first chunk: " + value);
    }

    @Test
    void twoJsonPointers() {
        JsonNode doc = splitJsonDocument("/text", "/more-text");
        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        assertEquals(4, chunks.size());
        String lastChunk = chunks.get(3).get("text").asText();
        assertTrue(lastChunk.endsWith("For details, see Choose a REST API Instance. Hello world."),
            "The last chunk should have the text from '/more-text' concatenated on, separated by a space. " +
                "Actual last chunk: " + lastChunk);
    }

    @Test
    void jsonPointerNoMatch() {
        JsonNode doc = splitJsonDocument("/no-match");
        assertFalse(doc.has("chunks"), "If the JSON Pointer expression doesn't match anything, no error should " +
            "be thrown, but rather no chunks will be produced.");
    }

    @Test
    void customSplitter() {
        AbstractWriteHandle newContent = new DocumentTextSplitter(
            new JsonPointerTextSelector(new String[]{"/text"}, null),
            new CustomSplitter(Map.of("textToReturn", "hello")),
            new DefaultChunkAssembler(new ChunkConfig.Builder().build())
        ).apply(readJsonDocument()).next().getContent();

        JsonNode doc = ((JacksonHandle) newContent).get();
        assertEquals("hello", doc.at("/chunks/0/text").asText());
    }

    private JsonNode splitJsonDocument(String... jsonPointers) {
        DocumentWriteOperation sourceDocument = readJsonDocument();
        DocumentWriteOperation output = newJsonSplitter(jsonPointers).apply(sourceDocument).next();
        return ((JacksonHandle) output.getContent()).get();
    }

    private XmlNode splitTextDocument(String xpath) {
        DocumentWriteOperation sourceDocument = readXmlDocument();
        DocumentWriteOperation output = newXmlSplitter(xpath).apply(sourceDocument).next();
        String xml = HandleAccessor.contentAsString(output.getContent());
        return new XmlNode(xml);
    }

    private DocumentTextSplitter newJsonSplitter(String... jsonPointers) {
        return new DocumentTextSplitter(
            new JsonPointerTextSelector(jsonPointers, null),
            DocumentSplitters.recursive(500, 0),
            new DefaultChunkAssembler(new ChunkConfig.Builder().build())
        );
    }

    private DocumentTextSplitter newXmlSplitter(String path) {
        return new DocumentTextSplitter(
            new DOMTextSelector(path, null),
            DocumentSplitters.recursive(500, 0),
            new DefaultChunkAssembler(new ChunkConfig.Builder().build())
        );
    }

    private DocumentWriteOperation readJsonDocument() {
        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }

    private DocumentWriteOperation readXmlDocument() {
        final String uri = "/marklogic-docs/java-client-intro.xml";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        StringHandle contentHandle = getDatabaseClient().newXMLDocumentManager().read(uri, metadata, new StringHandle());
        return new DocumentWriteOperationImpl(uri, metadata, contentHandle);
    }
}
