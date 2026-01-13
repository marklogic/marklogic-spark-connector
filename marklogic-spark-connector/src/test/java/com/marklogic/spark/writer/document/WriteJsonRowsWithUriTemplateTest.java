/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteJsonRowsWithUriTemplateTest extends AbstractIntegrationTest {

    @Test
    void zipOfJsonObjects() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/json-objects.zip")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/zip/{number}.json")
            .option(Options.WRITE_COLLECTIONS, "zip-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("zip-test", 2);

        JsonNode doc = readJsonDocument("/zip/1.json");
        assertEquals("text1", doc.get("parent").get("child").asText());

        doc = readJsonDocument("/zip/2.json");
        assertEquals("text2", doc.get("parent").get("child").asText());
    }


    @Test
    void zipWithJsonArray() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/spark-json/json-array.zip")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/zip/{/0/number}.json")
            .option(Options.WRITE_COLLECTIONS, "json-arrays")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-arrays", 1);
        ArrayNode array = (ArrayNode) readJsonDocument("/zip/1.json");
        assertEquals(2, array.size(), "A URI template can work against a JSON array by utilizing JSON Pointer " +
            "expressions to refer to specific elements in an array.");
    }

    @Test
    void jsonReadViaBinaryFileSource() {
        newSparkSession().read()
            .format("binaryFile")
            .load("src/test/resources/spark-json/single-object.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/zip/{number}.json")
            .option(Options.WRITE_COLLECTIONS, "binary-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize(
            "The Spark binaryFile data source should be supported in that our connector should be able to apply a " +
                "URI template against a JSON document read via the binaryFile data source.",
            "binary-test", 1
        );
        JsonNode doc = readJsonDocument("/zip/3.json");
        assertEquals("text", doc.get("parent").get("child").asText());
    }
}
