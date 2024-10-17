/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ReadJsonLinesFilesTest extends AbstractIntegrationTest {

    @Test
    void test() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines/nested-objects.txt");

        assertEquals(2, dataset.count(), "Should have one row for each line in the file.");

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_REPLACE, ".*json-lines,''")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-lines", 2);

        JsonNode doc = readJsonDocument("/nested-objects.txt-1.json");
        assertEquals(1, doc.get("id").asInt());
        assertEquals("blue", doc.at("/data/color").asText());
        assertEquals("world", doc.get("hello").asText());

        doc = readJsonDocument("/nested-objects.txt-2.json");
        assertEquals(2, doc.get("id").asInt());
        assertFalse(doc.has("hello"));
    }

    @Test
    void withUriTemplate() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines/nested-objects.txt")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_TEMPLATE, "/a/{id}.json")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-lines", 2);

        JsonNode doc = readJsonDocument("/a/1.json");
        assertEquals(1, doc.get("id").asInt());

        doc = readJsonDocument("/a/2.json");
        assertEquals(2, doc.get("id").asInt());
    }

    @Test
    void encoding() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .option(Options.READ_FILES_ENCODING, "ISO-8859-1")
            .load("src/test/resources/json-lines/objects-iso-8859-1.txt")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_REPLACE, ".*json-lines,''")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-lines", 2);

        JsonNode doc = readJsonDocument("/objects-iso-8859-1.txt-1.json");
        assertEquals("Istituto di Anatomia e Istologia Patologica, Università di Ferrara, Italy",
            doc.get("text").asText(), "Verifying that the encoded text is correctly read and written to MarkLogic.");
    }

    @Test
    void gzip() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/json-lines/nested-objects.txt.gz")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_REPLACE, ".*json-lines,''")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-lines", 2);
    }

    @Test
    void gzipWithoutCompressionOption() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines/nested-objects.txt.gz")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "json-lines")
            .option(Options.WRITE_URI_REPLACE, ".*json-lines,''")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("json-lines", 2);
    }
}
