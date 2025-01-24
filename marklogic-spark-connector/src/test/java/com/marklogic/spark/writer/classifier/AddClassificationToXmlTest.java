/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;

class AddClassificationToXmlTest extends AbstractIntegrationTest {

    /**
     * Verifies that when both a splitter and a semaphore server are specified,
     * then classification is added to both the document level and the chunks.
     */
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
        doc.assertElementExists("Expecting the root of the document to have a 'model:classification' child element", "/root/model:classification/model:URL");
        doc.assertElementExists("Expecting each chunk to have a 'model:classification' child element", "/root/model:chunks/model:chunk[1]/model:classification/model:URL");
    }

    /**
     * Verifies that when a semaphore server is specified but not a splitter,
     * then classification is still added to the document level.
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void addClassificationToXmlWithoutSplitting() {
        final String apiKey = System.getenv("SEMAPHORE_API_KEY");
        assertNotNull(apiKey);

        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
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
        doc.assertElementExists("Expecting the root of the document to have a 'model:classification' child element", "/root/model:classification/model:URL");
        doc.assertElementMissing("Expecting the chunk to not include a 'model:classification' child element", "/root/model:chunks/model:chunk[1]/model:classification");
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
        doc.assertElementMissing("Expecting the root of the document to not include a 'model:classification' child element", "/root/model:classification");
        doc.assertElementMissing("Expecting the chunk to not include a 'model:classification' child element", "/root/model:chunks/model:chunk[1]/model:classification");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void noHttpsSpecifiedShouldDefaultToHttpAndFail() {
        final String apiKey = System.getenv("SEMAPHORE_API_KEY");
        assertNotNull(apiKey);

        final DataFrameWriter<Row> dfw = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")
            .option(Options.WRITE_CLASSIFIER_PORT, "443")
            .option(Options.WRITE_CLASSIFIER_ENDPOINT, "/cls/dev/cs1/")
            .option(Options.WRITE_CLASSIFIER_APIKEY, apiKey)
            .option(Options.WRITE_CLASSIFIER_TOKEN_ENDPOINT, "token/")
            .mode(SaveMode.Append);

        ConnectorException exception = assertThrowsConnectorException(dfw::save);
        assertTrue(exception.getMessage().contains("CloudException thrown fetching token"), "Unexpected error: " + exception.getMessage());
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
