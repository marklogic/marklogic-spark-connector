/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

public class SplitDocumentsTest extends AbstractWriteTest {

    @Test
    void test() {
        newWriterForSingleRow()
            .option(Options.WRITE_DOCUMENT_SPLITTER_FIELD_NAME, "content")
            .option(Options.WRITE_DOCUMENT_SPLITTER_MAX_SEGMENT_SIZE, 1000)
            .save();

        String uri = getUrisInCollection(COLLECTION, 4).get(0);
        JsonNode doc = readJsonDocument(uri);
        System.out.println(doc.toPrettyString());
    }
}
