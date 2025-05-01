/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.embedding.Chunk;
import com.marklogic.spark.core.embedding.DOMChunkSelector;
import com.marklogic.spark.core.embedding.XmlChunkConfig;
import com.marklogic.spark.dom.NamespaceContextFactory;
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
            .selectChunks("/test.xml", new StringHandle(XML).withFormat(Format.XML))
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

        XmlChunkConfig xmlChunkConfig = new XmlChunkConfig(textExpression, null, null, NamespaceContextFactory.makeNamespaceContext(properties));
        List<Chunk> chunks = new DOMChunkSelector("/ex:root/ex:chunk", xmlChunkConfig)
            .selectChunks("/test.xml", new StringHandle(NAMESPACED_XML).withFormat(Format.XML))
            .getChunks();
        assertNotNull(chunks);
        assertEquals(1, chunks.size());

        String actualChunkText = chunks.get(0).getEmbeddingText();
        assertEquals(expectedChunkText, actualChunkText);
    }
}
