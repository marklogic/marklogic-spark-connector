/*
 * Copyright © 2024 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class WriteNullValuesTest extends AbstractIntegrationTest {

    @Test
    void jsonWithEmptyValues() {
        newSparkSession().read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("src/test/resources/csv-files/empty-values.csv")
            // Add the special file path column so we can verify it's not included in the JSON.
            .withColumn("marklogic_spark_file_path", new Column("_metadata.file_path"))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/a/{number}.json")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "false")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/a/1.json");
        assertEquals(1, doc.get("number").asInt());
        assertEquals("blue", doc.get("color").asText());
        assertEquals(JsonNodeType.NULL, doc.get("flag").getNodeType());
        assertEquals(3, doc.size(), "The file path column should not be included in the serialization.");

        doc = readJsonDocument("/a/2.json");
        assertEquals(2, doc.get("number").asInt());
        assertEquals(" ", doc.get("color").asText(), "Verifies that whitespace is retained by default.");
        assertFalse(doc.get("flag").asBoolean());
        assertEquals(3, doc.size(), "The file path column should not be included in the serialization.");
    }

    @Test
    void xmlWithEmptyValues() {
        newSparkSession().read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("src/test/resources/csv-files/empty-values.csv")
            .withColumn("marklogic_spark_file_path", new Column("_metadata.file_path"))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_XML_ROOT_NAME, "test")
            .option(Options.WRITE_URI_TEMPLATE, "/a/{number}.xml")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "false")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/a/1.xml");
        doc.assertElementValue(
            "The empty flag column should be retained due to ignoreNullFields=true",
            "/test/flag", "");
        doc.assertElementValue("/test/number", "1");
        doc.assertElementValue("/test/color", "blue");

        // This is oddly misleading and won't show the whitespace in an element.
        doc = readXmlDocument("/a/2.xml");
        doc.assertElementValue("/test/number", "2");
        doc.assertElementValue("/test/color", " ");
        doc.assertElementValue("/test/flag", "false");
    }

    @Test
    void jsonLinesWithNestedFieldsConvertedToXml() {
        newSparkSession().read()
            .option("ignoreNullFields", "false")
            .json("src/test/resources/json-lines/nested-objects.txt")
            .withColumn("marklogic_spark_file_path", new Column("_metadata.file_path"))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_XML_ROOT_NAME, "parent")
            .option(Options.WRITE_URI_TEMPLATE, "/a/{id}.xml")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "false")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/a/1.xml");
        doc.assertElementValue("/parent/data/color", "blue");
        doc.assertElementValue("/parent/data/numbers[1]", "1");
        doc.assertElementValue("/parent/data/numbers[2]", "2");
        doc.assertElementValue("/parent/hello", "world");
        doc.assertElementValue("/parent/id", "1");

        doc = readXmlDocument("/a/2.xml");
        doc.assertElementValue(
            "'hello' is added even though it doesn't exist on the line. This is due to ignoreNullFields being false " +
                "and Spark adding 'hello' to the schema since it appears on the first line.",
            "/parent/hello", "");
    }
}
