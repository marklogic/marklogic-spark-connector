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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AddClassificationToXmlTest extends AbstractIntegrationTest {

    private static final String API_KEY = System.getenv("SEMAPHORE_API_KEY");

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void chunkAndAddClassificationToXmlInOriginalDoc() {
        readAndStartWrite()
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementExists("Expecting each chunk to have a 'model:classification' child element",
            "/root/model:chunks/model:chunk[1]/model:classification/model:URL");
        doc.assertElementExists("Expecting the root of the document to have a 'model:classification' child element",
            "/root/model:classification/model:URL");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void sidecarChunksAddClassificationToXml() {
        readAndStartWrite()
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementExists("Expecting the root of the document to have a 'model:classification' child element", "/root/model:classification/model:URL");

        doc = readXmlDocument("/split-test.xml-chunks-1.xml");
        doc.assertElementExists("Expecting each chunk to have a 'model:classification' child element", "/model:root/model:chunks/model:chunk[1]/model:classification/model:URL");
    }

    /**
     * Verifies that when a semaphore server is not specified, classification is not added to chunks.
     */
    @Test
    void noClassificationAddedToXmlWhenNoSemaphoreServerSpecified() {
        readAndStartWrite()
            .option(Options.WRITE_CLASSIFIER_HOST, "")
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementMissing("Expecting the chunk to not include a 'model:classification' child element",
            "/root/model:chunks/model:chunk[1]/model:classification");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void noHttpsSpecifiedShouldDefaultToHttpAndFail() {
        DataFrameWriter writer = readAndStartWrite()
            .option(Options.WRITE_CLASSIFIER_HTTP, true)
            .mode(SaveMode.Append);

        ConnectorException exception = assertThrowsConnectorException(writer::save);
        assertTrue(exception.getMessage().contains("CloudException thrown fetching token"),
            "Unexpected error: " + exception.getMessage());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SEMAPHORE_API_KEY", matches = ".*")
    void classifyXmlContentsWithoutChunking() {
        readAndStartWrite()
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

    private DataFrameWriter readAndStartWrite() {
        return readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_CLASSIFIER_APIKEY, API_KEY)
            .option(Options.WRITE_CLASSIFIER_HOST, "demo.data.progress.cloud")

            // Use paths that don't begin with a "/", to further ensure that a "/" is added automatically.
            .option(Options.WRITE_CLASSIFIER_PATH, "cls/dev/cs1")
            .option(Options.WRITE_CLASSIFIER_TOKEN_PATH, "token")

            // Defining these properties, even though they use the default values, just to get a little more
            // coverage with them.
            .option(Options.WRITE_CLASSIFIER_PORT, 443)
            .option(Options.WRITE_CLASSIFIER_HTTP, false);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }
}
