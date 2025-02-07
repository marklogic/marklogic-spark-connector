/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRowsWithJsonRootNameTest extends AbstractWriteTest {

    @Test
    void test() {
        newWriter()
            .option(Options.WRITE_JSON_ROOT_NAME, "myRootName")
            .option(Options.WRITE_URI_TEMPLATE, "/myRootNameTest/{/myRootName/docNum}.json")
            .save();

        // Verify a few of the docs to ensure the rootName was used correctly.
        for (int i = 1; i <= 3; i++) {
            String uri = String.format("/myRootNameTest/%d.json", i);
            JsonNode doc = readJsonDocument(uri, COLLECTION);
            assertTrue(doc.has("myRootName"));
            assertEquals(i, doc.get("myRootName").get("docNum").asInt());
            assertEquals("doc" + i, doc.get("myRootName").get("docName").asText());
        }
    }

    /**
     * Per https://restfulapi.net/valid-json-key-names/ , it appears that any valid, escaped string works as a
     * field name. So this verifies that a potentially bad field name is escaped correctly. Implementation-wise, we
     * expect that to be handled automatically by the Jackson library.
     */
    @Test
    void rootNameThatNeedsEscaping() {
        final String weirdRootName = "{is-'this\"-valid?{";
        newWriter()
            .option(Options.WRITE_JSON_ROOT_NAME, weirdRootName)
            .save();

        // Verify one document to ensure the weird root name is valid.
        String uri = getUrisInCollection(COLLECTION, 200).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has(weirdRootName));
        assertTrue(doc.get(weirdRootName).has("docNum"));
        assertTrue(doc.get(weirdRootName).has("docName"));
    }
}
