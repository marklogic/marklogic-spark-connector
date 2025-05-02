/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

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
    void withUriTemplateThatDoesntEndInJson() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines/nested-objects.txt");

        dataset.collectAsList().forEach(row -> assertEquals("JSON", row.getString(2),
            "The format column should default to 'JSON' since we know we're reading from a JSON Lines file."));
        
        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/a/{id}")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/a/1");
        assertEquals(1, doc.get("id").asInt());

        doc = readJsonDocument("/a/2");
        assertEquals(2, doc.get("id").asInt());

        Stream.of("/a/1", "/a/2").forEach(uri -> {
            String response = getDatabaseClient().newServerEval()
                .javascript(String.format("xdmp.nodeKind(cts.doc('%s').toObject())", uri))
                .evalAs(String.class);
            assertEquals("object", response, "While the URI template does not define an extension, the document " +
                "should still be stored in MarkLogic with a document type of JSON due to the 'format' column being " +
                "set to JSON. If it were a binary, this eval call would fail.");
        });

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
