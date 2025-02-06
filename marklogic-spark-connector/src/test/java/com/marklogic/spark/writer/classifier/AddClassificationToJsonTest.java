/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.udf.TextClassifierUdf;
import com.marklogic.spark.udf.TextSplitterConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AddClassificationToJsonTest extends AbstractIntegrationTest {

    private static final String API_KEY = System.getenv("SEMAPHORE_API_KEY");
    private static final String CLASSIFED_TEXT_COLUMN_NAME = "classificationResponse";
    private static final String CHUNKS_CLASSIFED_TEXT_COLUMN_NAME = "chunkClassifications";
    private static final String CHUNKS_COLUMN_NAME = "chunks";

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void chunkAndAddClassificationToJsonInOriginalJsonDoc() {
        assertNotNull(API_KEY);

        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");

        TextSplitterConfig splitterConfig = new TextSplitterConfig();
        splitterConfig.setMaxChunkSize(500);
        splitterConfig.setMaxOverlapSize(10);
        splitterConfig.setJsonPointers(List.of("/text\n/more-text"));
        UserDefinedFunction splitter = splitterConfig.buildUDF();

        readDocument("/marklogic-docs/java-client-intro.json")
            .withColumn(CHUNKS_COLUMN_NAME, splitter.apply(new Column("content")))
            .withColumn(CHUNKS_CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column(CHUNKS_COLUMN_NAME)))
            .withColumn(CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));
        assertTrue(doc.get("chunks").get(0).get("classification").has("URL"));
    }

    /**
     * Tests the use case where a user wants to split the text into chunks and classify each chunk, all
     * as part of one write process.
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void sidecarChunksAddClassificationToJson() {
        assertNotNull(API_KEY);

        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");

        TextSplitterConfig splitterConfig = new TextSplitterConfig();
        splitterConfig.setMaxChunkSize(500);
        splitterConfig.setMaxOverlapSize(10);
        splitterConfig.setJsonPointers(List.of("/text\n/more-text"));
        UserDefinedFunction splitter = splitterConfig.buildUDF();

        readDocument("/marklogic-docs/java-client-intro.json")
            .withColumn(CHUNKS_COLUMN_NAME, splitter.apply(new Column("content")))
            .withColumn(CHUNKS_CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column(CHUNKS_COLUMN_NAME)))
            .withColumn(CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));

        doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertTrue(doc.get("chunks").get(0).get("classification").has("URL"));
        assertTrue(doc.get("chunks").get(1).get("classification").has("URL"));
    }

    /**
     * Verifies that when a semaphore server is not specified, classification is not added to chunks.
     */
    @Test
    void noClassificationAddedToJsonWhenNoSemaphoreServerSpecified() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks-1.json");
        assertFalse(doc.get("chunks").get(0).has("classification"));
        assertFalse(doc.get("chunks").get(1).has("classification"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void classifyJsonContentsWithoutChunking() {
        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");
        Dataset<Row> dataset = readDocument("/marklogic-docs/java-client-intro.json")
            .withColumn(CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column("content")));

        assertEquals(1, dataset.count(), "Expecting 1 file");
        assertNotNull(dataset.col(CLASSIFED_TEXT_COLUMN_NAME));
        assertEquals(9, dataset.collectAsList().get(0).size(),
            format("Expecting the 8 standard columns for representing a column, plus the '%s' column.", CLASSIFED_TEXT_COLUMN_NAME));

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));
        assertFalse(doc.has("chunks"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void classifyContentsWithUdfWithoutClassifyingChunk() {
        assertNotNull(API_KEY);

        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");

        TextSplitterConfig splitterConfig = new TextSplitterConfig();
        splitterConfig.setMaxChunkSize(500);
        splitterConfig.setMaxOverlapSize(10);
        splitterConfig.setJsonPointers(List.of("/text\n/more-text"));
        UserDefinedFunction splitter = splitterConfig.buildUDF();

        readDocument("/marklogic-docs/java-client-intro.json")
            .withColumn(CHUNKS_COLUMN_NAME, splitter.apply(new Column("content")))
            .withColumn(CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.get("classification").has("URL"));
        assertFalse(doc.get("chunks").get(1).has("classification"));
    }

    @Test
    void chunkAndAddClassificationOnlyToChunksInJsonOriginalJsonDoc() {
        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");

        TextSplitterConfig splitterConfig = new TextSplitterConfig();
        splitterConfig.setMaxChunkSize(500);
        splitterConfig.setMaxOverlapSize(10);
        splitterConfig.setJsonPointers(List.of("/text\n/more-text"));
        UserDefinedFunction splitter = splitterConfig.buildUDF();

        readDocument("/marklogic-docs/java-client-intro.json")
            .withColumn(CHUNKS_COLUMN_NAME, splitter.apply(new Column("content")))
            .withColumn(CHUNKS_CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column(CHUNKS_COLUMN_NAME)))
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on max chunk size of 500.");
    }

    @Test
    @Disabled("Placeholder for a future test for data that is not UTF-8")
    void classifyNonUtf8JsonData() {
        assertTrue(true);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
