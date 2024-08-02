/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Purpose of this class is to demonstrate how the Spark JSON data source -
 * https://spark.apache.org/docs/latest/sql-data-sources-json.html - can be used with our connector. There are 3
 * separate use cases we can support - 1) Treat each JSON file as a separate row; 2) Treat each object in an array in
 * a JSON file as a separate row; and 3) Use the 'JSON Lines' support for a JSON lines file, where each line becomes
 * a row. In each scenario, we write each row as a document in MarkLogic.
 * <p>
 * This test therefore doesn't test any code in our connector that isn't tested elsewhere. It's simply demonstrating
 * how Spark JSON can be used with our connector.
 */
class WriteSparkJsonTest extends AbstractWriteTest {

    /**
     * The default behavior of Spark JSON is that each line is expected to be a separate JSON object. Each line then
     * becomes a row, which then gets written as a separate document to MarkLogic.
     */
    @Test
    void eachLineInJsonLinesFileBecomesADocument() {
        newSparkSession().read().format("json")
            .load("src/test/resources/spark-json/json-lines.txt")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/spark-json/{number}.json")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "true")
            .mode(SaveMode.Append)
            .save();

        verifyTheTwoJsonDocuments();
    }

    /**
     * Happily, when Spark JSON is used with multiLine=true on a file containing an array of objects, Spark will
     * read each object as a separate row. This allows for each object to become a separate document in MarkLogic.
     * If the user does not want this behavior but rather wants the entire file to become a document, they need to use
     * Spark's binaryFile data source as shown in a test below this one.
     * <p>
     * Note that if a user attempts to read an array file without multiLine=true, they'll get this error:
     * "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
     * referenced columns only include the internal corrupt record column
     * (named _corrupt_record by default)."
     */
    @Test
    void eachObjectInArrayBecomesADocument() {
        newSparkSession().read().format("json")
            .option("multiLine", true)
            .load("src/test/resources/spark-json/array-of-objects.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/spark-json/{number}.json")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "true")
            .mode(SaveMode.Append)
            .save();

        verifyTheTwoJsonDocuments();
    }

    /**
     * This test shows that the same multiLine=true reader can be used for both single object files and files containing
     * an array of objects.
     */
    @Test
    void singleObjectFileAndArrayOfObjectsFile() {
        newSparkSession().read().format("json")
            .option("multiLine", true)
            .load(
                "src/test/resources/spark-json/array-of-objects.json",
                "src/test/resources/spark-json/single-object.json"
            )
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/spark-json/{number}.json")
            .option(Options.WRITE_JSON_SERIALIZATION_OPTION_PREFIX + "ignoreNullFields", "true")
            .mode(SaveMode.Append)
            .save();

        // Verifies the two objects from the array.
        verifyTheTwoJsonDocuments();

        // And verify that the object from single-object.json was created as well.
        ObjectNode doc = (ObjectNode) readJsonDocument("/spark-json/3.json");
        assertEquals(3, doc.get("number").asInt());
        assertEquals("text", doc.get("parent").get("child").asText());
    }

    /**
     * If the user has a JSON file that is an object, the Spark JSON data source can be used with multiLine=true.
     * The benefit is that the fields in the JSON object become Spark columns, thereby making them accessible to our
     * URI template feature.
     */
    @Test
    void jsonObjectFileBecomesDocument() {
        newSparkSession().read().format("json")
            .option("multiLine", true)
            .load("src/test/resources/spark-json/single-object.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/spark-json/{number}.json")
            .mode(SaveMode.Append)
            .save();

        ObjectNode doc = (ObjectNode) readJsonDocument("/spark-json/3.json");
        assertEquals(3, doc.get("number").asInt());
        assertEquals("text", doc.get("parent").get("child").asText());
    }

    /**
     * If the user wants a JSON array file to become a single document, they need to use Spark's Binary data source
     * so that the document is ingested as-is. This prohibits the use of our "URI template" feature, but that's likely
     * not useful for a file containing an array as opposed to a file containing an object.
     */
    @Test
    void jsonArrayFileBecomesADocument() {
        newSparkSession().read().format("binaryFile")
            .load("src/test/resources/spark-json/array-of-objects.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "spark-file")
            .mode(SaveMode.Append)
            .save();

        String uri = getUrisInCollection("spark-file", 1).get(0);
        ArrayNode doc = (ArrayNode) readJsonDocument(uri);
        assertEquals(2, doc.size(), "Expecting the file to be ingested as-is, so there should be an array with " +
            "2 objects in it.");
    }

    private void verifyTheTwoJsonDocuments() {
        ObjectNode doc = (ObjectNode) readJsonDocument("/spark-json/1.json");
        assertEquals(1, doc.get("number").asInt());
        assertEquals("world", doc.get("hello").asText());
        assertEquals(2, doc.size(), "Should only have 'number' and 'hello' fields.");

        doc = (ObjectNode) readJsonDocument("/spark-json/2.json");
        assertEquals(2, doc.get("number").asInt());
        assertEquals("This is different from the first object.", doc.get("description").asText());
        assertEquals(2, doc.size(), "Should only have 'number' and 'description' fields.");
    }
}
