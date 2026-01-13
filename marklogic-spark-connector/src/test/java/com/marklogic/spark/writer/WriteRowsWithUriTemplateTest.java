/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WriteRowsWithUriTemplateTest extends AbstractWriteTest {

    @Test
    void validUriTemplateWithTwoColumns() {
        newWriter()
            .option(Options.WRITE_URI_TEMPLATE, "/test/{docNum}/{docName}.json")
            // These are expected to be ignored, with warnings logged.
            .option(Options.WRITE_URI_PREFIX, "/should/be/ignored/")
            .option(Options.WRITE_URI_SUFFIX, ".xml")
            .save();

        assertCollectionSize(COLLECTION, 200);
        for (int i = 1; i <= 200; i++) {
            final String expectedUri = String.format("/test/%d/doc%d.json", i, i);
            assertInCollections(expectedUri, COLLECTION);
        }
    }

    @Test
    void someDocumentsAreMissingFields() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines/missing-fields.txt")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_TEMPLATE, "/a/{id}.json")
            .option(Options.WRITE_URI_TEMPLATE_WARN_ON_MISSING_FIELD, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Each document should be created, but two of the documents will have UUIDs in the URI " +
            "instead of the 'id' value since they don't have an 'id' value. This behavior was introduced in release " +
            "3.0.0 to provide a better default user experience for when some documents unexpectedly do not have " +
            "the specified value.", "json-lines", 4);

        List<String> uris = getUrisInCollection("json-lines", 4);
        assertTrue(uris.contains("/a/1.json"));
        assertTrue(uris.contains("/a/3.json"));
        uris.forEach(uri -> {
            if (!uri.equals("/a/1.json") && !uri.equals("/a/3.json")) {
                String replacement = uri.replaceAll("/a/", "").replaceAll(".json", "");
                assertTrue(replacement.startsWith("UNRESOLVED-"), "When a field cannot be resolved in a URI " +
                    "template, and the warnOnMissingField option is set, the connector should replace the " +
                    "expression with 'UNRESOLVED-' followed by a UUID. Actual URI: " + uri);


                UUID uuid = UUID.fromString(replacement.replaceAll("UNRESOLVED-", ""));
                assertNotNull(uuid, "Calling fromString ensures that the URI contains a UUID, which should be " +
                    "used when an expression in the URI template can not be resolved.");
            }
        });
    }

    @Test
    void closingBraceBeforeOpeningBrace() {
        verifyTemplateIsInvalid("/test/}{docName}.json", "closing brace found before opening brace");
    }

    @Test
    void bracesWithNoColumnName() {
        verifyTemplateIsInvalid("/test/{}.json", "no column name within opening and closing brace");
    }

    @Test
    void missingClosingBrace() {
        verifyTemplateIsInvalid("/test/{docNum/{docName}.json", "expected closing brace, but found opening brace");
    }

    @Test
    void openingBraceWithoutClosingBrace() {
        verifyTemplateIsInvalid("/test/{docNum}/{docName.json", "opening brace without closing brace");
    }

    @Test
    void columnNameDoesntExist() {
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriterForSingleRow()
                .option(Options.WRITE_URI_TEMPLATE, "/test/{id}/{doesntExist}.json")
                .save()
        );

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof RuntimeException, "Unexpected cause: " + cause);
        final String expectedMessage = "Expression 'doesntExist' did not resolve to a value in row: " +
            "{\"id\":\"1\",\"content\":\"hello world\"," +
            "\"systemStart\":\"2014-04-03T11:00:00\",\"systemEnd\":\"2014-04-03T16:00:00\"," +
            "\"validStart\":\"2014-04-03T11:00:00\",\"validEnd\":\"2014-04-03T16:00:00\"," +
            "\"columnWithOnlyWhitespace\":\"   \"}; " +
            "expression is required by URI template: /test/{id}/{doesntExist}.json";

        assertEquals(expectedMessage, cause.getMessage(), "The entire JSON row is being included in the error " +
            "message so that the user is able to figure out what a column they chose in the URI template isn't " +
            "populated in a particular row.");

    }

    @Test
    void columnWithOnlyWhitespace() {
        SparkException ex = assertThrows(
            SparkException.class,
            () -> newWriterForSingleRow()
                .option(Options.WRITE_URI_TEMPLATE, "/test/{id}/{columnWithOnlyWhitespace}.json")
                .save()
        );

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof RuntimeException, "Unexpected cause: " + cause);
    }

    private void verifyTemplateIsInvalid(String uriTemplate, String expectedMessage) {
        ConnectorException ex = assertThrowsConnectorException(
            () -> newWriter().option(Options.WRITE_URI_TEMPLATE, uriTemplate).save()
        );

        String message = ex.getMessage();
        expectedMessage = "Invalid value for " + Options.WRITE_URI_TEMPLATE + ": " + uriTemplate + "; " + expectedMessage;
        assertEquals(expectedMessage, message, "Unexpected error message: " + message);
    }
}
