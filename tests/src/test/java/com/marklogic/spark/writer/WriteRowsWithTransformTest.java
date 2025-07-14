/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.MarkLogicNamespaceProvider;
import com.marklogic.junit5.NamespaceProvider;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteRowsWithTransformTest extends AbstractWriteTest {

    @Override
    protected NamespaceProvider getNamespaceProvider() {
        // Overridden so XmlNode can be used below with the basic JSON namespace.
        return new MarkLogicNamespaceProvider(
            "jb", "http://marklogic.com/xdmp/json/basic"
        );
    }

    /**
     * This also verifies that a user can write XML documents without support from the connector. The user can use a
     * REST transform to convert the JSON representation of a Spark row into whatever XML document they want, along with
     * giving it a more meaningful suffix of ".xml".
     */
    @Test
    void transformToXml() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "transformToXml")
            // Surprisingly, the REST API is fine with writing an XML document with a suffix of ".json". But making this
            // a more realistic scenario, as a user would almost certainly want ".xml" instead.
            .option(Options.WRITE_URI_SUFFIX, ".xml")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        assertTrue(uri.endsWith(".xml"), "Unexpected URI: " + uri);

        XmlNode doc = readXmlDocument(uri);
        String message = "Verifying that one XML element exists, which is sufficient for knowing that the " +
            "JSON sent from the Spark connector was successfully converted into XML";
        doc.assertElementValue(message, "/jb:json/jb:content", "hello world");
    }

    @Test
    void withParams() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "withParams")
            .option(Options.WRITE_TRANSFORM_PARAMS, "param1,value1,param2,value2")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("params"), "Expected the withParams transform to toss all the params into the document");
        assertEquals("value1", doc.get("params").get("param1").asText());
        assertEquals("value2", doc.get("params").get("param2").asText());
    }

    @Test
    void withParamsAndCustomDelimiter() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "withParams")
            .option(Options.WRITE_TRANSFORM_PARAMS, "param1;value,1;param2;value,2")
            .option(Options.WRITE_TRANSFORM_PARAMS_DELIMITER, ";")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("params"), "Expected the withParams transform to toss all the params into the document");
        assertEquals("value,1", doc.get("params").get("param1").asText());
        assertEquals("value,2", doc.get("params").get("param2").asText());
    }

    @Test
    void transformThrowsError() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "throwError")
            .save());

        assertTrue(ex.getMessage().contains("This is an intentional error for testing purposes."),
            "The transform is expected to throw an error which should be caught by " +
                "WriteBatcherDataWriter and then thrown as a ConnectorException. " +
                "Actual error: " + ex.getMessage());
    }

    @Test
    void invalidTransform() {
        SparkException ex = assertThrows(SparkException.class, () -> newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "this-doesnt-exist")
            .save());

        assertTrue(ex.getMessage().contains("Extension this-doesnt-exist or a dependency does not exist"),
            "The connector can't easily validate that a REST transform is valid, but the expectation is that the " +
                "error message from the REST API will make the problem evident to the user; " +
                "unexpected message: " + ex.getMessage());
    }

    @Test
    void invalidTransformParams() {
        ConnectorException ex = assertThrowsConnectorException(
            () -> newWriterForSingleRow()
                .option(Options.WRITE_TRANSFORM_NAME, "withParams")
                .option(Options.WRITE_TRANSFORM_PARAMS, "param1,value1,param2")
                .save());

        assertEquals(
            "The spark.marklogic.write.transformParams option must contain an equal number of parameter names and values; received: param1,value1,param2",
            ex.getMessage()
        );
    }
}
