/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.AbstractWriteTest;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessMultipleRowsWithCustomCodeTest extends AbstractWriteTest {

    @Test
    void multipleItems() {
        // A single string column is considered the "default" schema, even if the column name isn't "URI".
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '').select(['LastName'])")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_BATCH_SIZE, 5)
            .option(Options.WRITE_EXTERNAL_VARIABLE_NAME, "LAST_NAMES")
            .option(Options.WRITE_INVOKE, "/processMultipleUris.sjs")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize(
            "Expecting 15 docs in the collection used by processMultipleUris.sjs, one for each customer row",
            "process-multiple-test", 15);
        JsonNode doc = readJsonDocument("/multiple/Awton.json");
        assertEquals("Awton", doc.get("LastName").asText());
    }

    @Test
    void customDelimiter() {
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '').select(['LastName'])")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_BATCH_SIZE, 5)
            .option(Options.WRITE_EXTERNAL_VARIABLE_NAME, "LAST_NAMES")
            .option(Options.WRITE_EXTERNAL_VARIABLE_DELIMITER, ";")
            .option(Options.WRITE_INVOKE, "/processMultipleUrisCustomDelimiter.sjs")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize(
            "Expecting 15 docs in the collection used by processMultipleUris.sjs, one for each customer row",
            "process-multiple-test", 15);
        JsonNode doc = readJsonDocument("/multiple/Awton.json");
        assertEquals("Awton", doc.get("LastName").asText());
    }

    @Test
    void customSchema() {
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical', 'Authors', '').select(['CitationID', 'LastName'])")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_BATCH_SIZE, 5)
            .option(Options.WRITE_EXTERNAL_VARIABLE_NAME, "authors")
            .option(Options.WRITE_INVOKE, "/processMultipleObjects.sjs")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize(
            "Expecting 15 docs in the collection used by processMultipleObjects.sjs, one for each customer row",
            "custom-schema-test", 15);
        JsonNode doc = readJsonDocument("/multiple/1/Awton.json");
        assertEquals(1, doc.get("CitationID").asInt());
        assertEquals("Awton", doc.get("LastName").asText());
    }
}
