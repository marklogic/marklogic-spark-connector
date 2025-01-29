/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.udf.TextClassifierUdf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;

class AddClassificationToXmlTest extends AbstractIntegrationTest {

    private static final String API_KEY = System.getenv("SEMAPHORE_API_KEY");
    private static final String CLASSIFED_TEXT_COLUMN_NAME = "classificationResponse";

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void splitToSeparateDocumentsAndAddClassificationToXml() {
        final String apiKey = System.getenv("SEMAPHORE_API_KEY");
        assertNotNull(apiKey);

        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_HTTPS, true)
            .option(Options.WRITE_CLASSIFIER_PORT, "443")
            .option(Options.WRITE_CLASSIFIER_ENDPOINT, "/cls/dev/cs1/")
            .option(Options.WRITE_CLASSIFIER_APIKEY, apiKey)
            .option(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementExists("Expecting each chunk to have a 'model:classification' child element", "/root/model:chunks/model:chunk[1]/model:classification/model:URL");
    }

    /**
     * Verifies that when a semaphore server is not specified, classification is not added to chunks.
     */
    @Test
    void noClassificationAddedWhenNoSemaphoreServerSpecified() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementMissing("Expecting the chunk to not include a 'model:classification' child element", "/root/model:chunks/model:chunk[1]/model:classification");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void noHttpsSpecifiedShouldDefaultToHttpAndFail() {
        final DataFrameWriter<Row> dfw = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_PORT, "443")
            .option(Options.WRITE_CLASSIFIER_ENDPOINT, "/cls/dev/cs1/")
            .option(Options.WRITE_CLASSIFIER_APIKEY, API_KEY)
            .option(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/")
            .mode(SaveMode.Append);

        ConnectorException exception = assertThrowsConnectorException(dfw::save);
        assertTrue(exception.getMessage().contains("CloudException thrown fetching token"), "Unexpected error: " + exception.getMessage());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void classifyXmlContentsWithUdf() {
        final UserDefinedFunction textClassifierUdf = TextClassifierUdf.build(
            "demo.data.progress.cloud", true, "443", "/cls/dev/cs1/", API_KEY, "token/");
        Dataset<Row> dataset = readDocument("/marklogic-docs/java-client-intro.xml")
            .withColumn(CLASSIFED_TEXT_COLUMN_NAME, textClassifierUdf.apply(new Column("content")));

        assertEquals(1, dataset.count(), "Expecting 1 file");
        assertNotNull(dataset.col(CLASSIFED_TEXT_COLUMN_NAME));
        assertEquals(9, dataset.collectAsList().get(0).size(),
            format("Expecting the 8 standard columns for representing a column, plus the '%s' column.", CLASSIFED_TEXT_COLUMN_NAME));

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementExists("Expecting the root of the document to have a 'model:classification' child element", "/root/model:classification/model:URL");
    }

    @Test
    @Disabled("Placeholder for a future test for data that is not UTF-8")
    void classifyNonUtf8XmlData() {
        assertTrue(true);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
