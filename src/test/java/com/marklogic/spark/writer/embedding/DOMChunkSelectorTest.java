/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DOMChunkSelectorTest {

    private static final String CHUNKS_EXPRESSION = "/root/chunk";

    private static final String XML = "<root>" +
        "<chunk>" +
        "<text hidden='false'>Hello <b>bold text</b></text>" +
        "<other>Other text</other>" +
        "<status enabled='true'/>" +
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
        String actualChunkText = new DOMChunkSelector(CHUNKS_EXPRESSION, textExpression)
            .selectChunks(makeDocument(XML))
            .getChunks().get(0).getEmbeddingText();

        assertEquals(expectedChunkText, actualChunkText);
    }

    private DocumentWriteOperation makeDocument(String xml) {
        return new DocumentWriteOperationImpl(
            "/test.xml", new DocumentMetadataHandle(),
            new StringHandle(xml).withFormat(Format.XML)
        );
    }
}
