/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
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

class AddConceptsToXmlTest extends AbstractIntegrationTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void splitToSeparateDocumentsAndAddConcepts() {
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
        doc.assertElementExists("Expecting each chunk to have a 'model:concepts' child element", "/root/model:chunks/model:chunk[1]/model:concepts");
    }

    /**
     * Verifies that when a semaphore server is not specified, concepts are not added to chunks.
     */
    @Test
    void noConceptsAddedWhenNoSemaphoreServerSpecified() {
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
        doc.assertElementMissing("Expecting the chunk to not include a 'model:concepts' child element", "/root/model:chunks/model:chunk[1]/model:concepts");
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
        assertTrue(exception.getMessage().contains("Error retrieving token"), "Unexpected error: " + exception.getMessage());
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
