/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.dom.XPathNamespaceContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DOMChunkSelectorTest {

    private static final String XML = "<root>" +
        "<chunk>" +
        "<text hidden='false'>Hello <b>bold text</b></text>" +
        "<other>Other text</other>" +
        "<status enabled='true'/>" +
        "</chunk>" +
        "</root>";

    private static final String NAMESPACED_XML = "<root xmlns='org:example'>" +
        "<chunk>" +
        "<text hidden='false'>Hello <b>bold text</b></text>" +
        "</chunk>" +
        "</root>";

    @ParameterizedTest
    @CsvSource({
        "text,Hello bold text",
        "node()/@*,false true",
        "node(),Hello bold text Other text",
        "node()/text(),Hello  Other text",
        "node()[self::text or self::other]//text(),Hello  bold text Other text"
    })
    void test(String textExpression, String expectedChunkText) {
        XmlChunkConfig xmlChunkConfig = new XmlChunkConfig(textExpression, null, null, null);
        String actualChunkText = new DOMChunkSelector("/root/chunk", xmlChunkConfig)
            .selectChunks(makeDocument(XML))
            .getChunks().get(0).getEmbeddingText();

        assertEquals(expectedChunkText, actualChunkText);
    }

    @ParameterizedTest
    @CsvSource({
        "ex:text,Hello bold text"
    })
    void namespaceTest(String textExpression, String expectedChunkText) {
        Map<String, String> properties = new HashMap<>();
        properties.put(Options.XPATH_NAMESPACE_PREFIX + "ex", "org:example");

        XmlChunkConfig xmlChunkConfig = new XmlChunkConfig(textExpression, null, null, new XPathNamespaceContext(properties));
        List<Chunk> chunks = new DOMChunkSelector("/ex:root/ex:chunk", xmlChunkConfig).selectChunks(makeDocument(NAMESPACED_XML)).getChunks();
        assertNotNull(chunks);
        assertEquals(1, chunks.size());

        String actualChunkText = chunks.get(0).getEmbeddingText();
        assertEquals(expectedChunkText, actualChunkText);
    }

    private DocumentWriteOperation makeDocument(String xml) {
        return new DocumentWriteOperationImpl(
            "/test.xml", new DocumentMetadataHandle(),
            new StringHandle(xml).withFormat(Format.XML)
        );
    }
}
