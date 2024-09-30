/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * In this context, "streaming" != Spark Structured Streaming, but rather avoiding reading the contents of a file
 * into memory by postponing reading of the file until the writer phase, where it can then be streamed from disk into
 * MarkLogic.
 */
class ReadGenericFilesStreamingTest extends AbstractIntegrationTest {

    @Test
    void stream() throws Exception {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/mixed-files");

        assertEquals(4, dataset.count());
        verifyEachRowHasFileContextAsItsContent(dataset);

        defaultWrite(dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.WRITE_LOG_PROGRESS, 1) // Included for manual verification of logging.
            .option(Options.WRITE_COLLECTIONS, "streamed-files")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''"));

        assertCollectionSize("This verifies that enabling streaming does not break any functionality. We don't " +
            "have a test for a file large enough to warrant streaming as that would drastically slow down the suite " +
            "of tests.", "streamed-files", 4);
    }

    @Test
    void streamFileWithSpacesInFilename() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/generic-files/with-spaces/three uris.csv");

        defaultWrite(dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.WRITE_COLLECTIONS, "streamed-files")
            .option(Options.WRITE_URI_REPLACE, ".*/with-spaces,''"));

        String uri = getUrisInCollection("streamed-files", 1).get(0);
        assertEquals("/three%20uris.csv", uri, "Due to bug MLE-17088, the PUT v1/documents endpoint is not able to " +
            "accept a URI with a space in it. So when streaming files with spaces in them, the URIs will have " +
            "encoded spaces in them.");
    }

    @Test
    void handleFailureWhileStreaming() {
        DataFrameWriter writer = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/mixed-files/hello.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.STREAM_FILES, true)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, "not-an-actual-role,read")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("SEC-ROLEDNE: xdmp:role(\"not-an-actual-role\")"),
            "This verifies that when the connector uses GenericDocumentManager to PUT a single document, any error " +
                "is still wrapped in a ConnectorException. Actual error message: " + ex.getMessage());
    }

    private void verifyEachRowHasFileContextAsItsContent(Dataset<Row> dataset) throws Exception {
        for (Row row : dataset.collectAsList()) {
            byte[] content = (byte[]) row.get(1);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(content))) {
                FileContext fileContext = (FileContext) ois.readObject();
                assertNotNull(fileContext, "To enable streaming of files, the content column should not " +
                    "contain the contents of the file, which forces reading the entire file into memory. " +
                    "Instead, the associated FileContext - containing the Hadoop SerializableConfiguration class - " +
                    "should be serialized so that it can be used to read the file during the writer phase.");
            }
        }
    }
}
