/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.AbstractWriteTest;
import com.marklogic.spark.writer.MarkLogicWrite;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ProcessWithCustomCodeTest extends AbstractWriteTest {

    @Test
    void logProgressTest() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_XQUERY, "for $i in 1 to 100 return $i")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            // With "uneven" numbers like this, the user will still see 5 progress entries, but the counts won't even -
            // they'll be 24, 40, 64, 80, and 100.
            .option(Options.WRITE_BATCH_SIZE, 8)
            .option(Options.WRITE_LOG_PROGRESS, 20)
            .option(Options.WRITE_JAVASCRIPT, "var URI; console.log('Nothing to do here.')")
            .mode(SaveMode.Append)
            .save();

        assertTrue(true, "No assertion needed, this test is only for manual inspection of the progress log entries.");
    }

    @Test
    void invokeJavaScript() {
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_INVOKE, "/processUri.sjs")
            .save();

        verifyThreeJsonDocumentsWereWritten();
    }

    @Test
    void evalJavaScript() {
        // For easier testing, this uses the same code as what's in processUri.sjs so the same assertions can be made.
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_JAVASCRIPT, "declareUpdate(); var URI; " +
                "xdmp.documentInsert(URI + '.json', {\"hello\":\"world\"}, " +
                "{\"permissions\": [xdmp.permission(\"spark-user-role\", \"read\"), xdmp.permission(\"spark-user-role\", \"update\")]});")
            .save();

        verifyThreeJsonDocumentsWereWritten();
    }

    @Test
    void evalJavaScriptFile() {
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_JAVASCRIPT_FILE, "src/test/resources/custom-code/my-writer.js")
            .save();

        verifyThreeJsonDocumentsWereWritten();
    }

    @Test
    void invokeXQuery() {
        newWriterWithDefaultConfig("three-uris.csv", 1)
            .option(Options.WRITE_INVOKE, "/process-uri.xqy")
            .save();

        verifyThreeXmlDocumentsWereWritten();
    }

    @Test
    void evalXQuery() {
        // For easier testing, this uses the same code as what's in process-uri.xqy so the same assertions can be made.
        newWriterWithDefaultConfig("three-uris.csv", 1)
            .option(Options.WRITE_XQUERY, "declare variable $URI external;\n" +
                "xdmp:document-insert($URI || \".xml\", <hello>world</hello>,\n" +
                "(xdmp:permission(\"spark-user-role\", \"read\"),\n" +
                "xdmp:permission(\"spark-user-role\", \"update\")\n" +
                "));")
            .save();

        verifyThreeXmlDocumentsWereWritten();
    }

    @Test
    void evalXQueryFile() {
        newWriterWithDefaultConfig("three-uris.csv", 1)
            .option(Options.WRITE_XQUERY_FILE, "src/test/resources/custom-code/my-writer.xqy")
            .save();

        verifyThreeXmlDocumentsWereWritten();
    }

    @Test
    void customExternalVariableName() {
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_EXTERNAL_VARIABLE_NAME, "MY_VAR")
            .option(Options.WRITE_JAVASCRIPT, "declareUpdate(); var MY_VAR; " +
                "xdmp.documentInsert(MY_VAR + '.json', {\"hello\":\"world\"}, " +
                "{\"permissions\": [xdmp.permission(\"spark-user-role\", \"read\"), xdmp.permission(\"spark-user-role\", \"update\")]});")
            .save();

        verifyThreeJsonDocumentsWereWritten();
    }

    @Test
    void customSchema() {
        newDefaultReader()
            .option(Options.READ_OPTIC_QUERY,
                "op.fromView('Medical', 'Authors', '').select(['CitationID', 'LastName'])"
            )
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_EXTERNAL_VARIABLE_NAME, "author")
            .option(Options.WRITE_INVOKE, "/processObject.sjs")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize(
            "Expecting 15 docs in the collection used by processObject.sjs, one for each customer row",
            "custom-schema-test", 15);

        JsonNode doc = readJsonDocument("/temp/Awton.json");
        assertEquals(1, doc.get("CitationID").asInt());
        assertEquals("Awton", doc.get("LastName").asText());
    }

    @Test
    void userDefinedVariables() {
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_JAVASCRIPT, "declareUpdate(); var URI; var keyName; var keyValue;" +
                "const doc = {}; doc[keyName] = keyValue; " +
                "xdmp.documentInsert(URI + '.json', doc, " +
                "{\"permissions\": [xdmp.permission(\"spark-user-role\", \"read\"), xdmp.permission(\"spark-user-role\", \"update\")]});")
            .option(Options.WRITE_VARS_PREFIX + "keyName", "hello")
            .option(Options.WRITE_VARS_PREFIX + "keyValue", "world")
            .save();

        verifyThreeJsonDocumentsWereWritten();
    }

    @Test
    void abortOnFailure() {
        SparkException ex = assertThrows(SparkException.class, () ->
            newWriterWithDefaultConfig("three-uris.csv", 2)
                .option(Options.WRITE_JAVASCRIPT, "var URI; throw Error('Boom!');")
                .save()
        );

        assertTrue(ex.getMessage().contains("Error running JavaScript request: Error: Boom!"),
            "By default, any exception is expected to be propagated to Spark so that it can fail the job. " +
                "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void dontAbortOnFailure() {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        MarkLogicWrite.setSuccessCountConsumer(count -> successCount.set(count));
        MarkLogicWrite.setFailureCountConsumer(count -> failureCount.set(count));

        // The lack of an error here indicates that the job did not abort. The connector is expected to have logged
        // each error instead.
        newWriterWithDefaultConfig("three-uris.csv", 2)
            .option(Options.WRITE_JAVASCRIPT, "var URI; throw Error('Boom!');")
            .option(Options.WRITE_ABORT_ON_FAILURE, "false")
            .save();

        assertEquals(3, failureCount.get());
        assertEquals(0, successCount.get());
    }

    private void verifyThreeJsonDocumentsWereWritten() {
        Stream.of("/process-test1.json", "/process-test2.json", "/process-test3.json").forEach(uri -> {
            JsonNode doc = readJsonDocument(uri);
            assertEquals("world", doc.get("hello").asText());
        });
    }

    private void verifyThreeXmlDocumentsWereWritten() {
        Stream.of("/process-test1.xml", "/process-test2.xml", "/process-test3.xml").forEach(uri -> {
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementValue("/hello", "world");
        });
    }
}
