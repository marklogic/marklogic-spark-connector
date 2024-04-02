package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * So our problem is that if you have a zip of JSON lines files or a zip of CSV files, Spark can't handle those.
 */
class ReadJsonLinesFilesTest extends AbstractIntegrationTest {

    @Test
    void textFile() {
        List<Row> rows = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/spark-json/json-lines.txt")
            .collectAsList();

        assertEquals(2, rows.size());

        Row row1 = rows.get(0);
        assertTrue(row1.getString(0).endsWith("json-lines.txt-1.json"), "Unexpected URI: " + row1.getString(0));
        JsonNode content = readJsonFromDocumentRow(row1);
        assertEquals(1, content.get("number").asInt());
        assertEquals("json", row1.getString(2));

        Row row2 = rows.get(1);
        assertTrue(row2.getString(0).endsWith("json-lines.txt-2.json"), "Unexpected URI: " + row2.getString(0));
        content = readJsonFromDocumentRow(row2);
        assertEquals(2, content.get("number").asInt());
        assertEquals("json", row2.getString(2));
    }

    @Test
    void gzipFile() {
        List<Row> rows = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/spark-json/jsonLines.txt.gz")
            .collectAsList();

        assertEquals(2, rows.size());

        Row row1 = rows.get(0);
        assertTrue(row1.getString(0).endsWith("jsonLines.txt.gz-1.json"), "Unexpected URI: " + row1.getString(0));
        JsonNode content = readJsonFromDocumentRow(row1);
        assertEquals(1, content.get("number").asInt());
        assertEquals("json", row1.getString(2));

        Row row2 = rows.get(1);
        assertTrue(row2.getString(0).endsWith("jsonLines.txt.gz-2.json"), "Unexpected URI: " + row2.getString(0));
        content = readJsonFromDocumentRow(row2);
        assertEquals(2, content.get("number").asInt());
        assertEquals("json", row2.getString(2));
    }

   // 283022 -> 10.3s
    @Test
    void performance() {
        long start = System.currentTimeMillis();
        long count = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
//            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("/Users/rudin/data/json-lines")
            .count();
        System.out.println("TIME: " + (System.currentTimeMillis() - start));
        System.out.println("COUNT: " + count);
    }

    // 5 .json files = 283022 -> 7.8s.
    @Test
    void sparkJson() {
        long start = System.currentTimeMillis();
        newSparkSession()
            .read().format("json")
            .load("src/test/resources/spark-json/jsonLinesOneBadLine.txt")
            .show();
        System.out.println("TIME: " + (System.currentTimeMillis() - start));
    }

}
