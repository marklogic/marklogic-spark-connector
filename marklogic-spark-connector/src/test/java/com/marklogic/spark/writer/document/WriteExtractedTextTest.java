/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.udf.TextExtractor;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteExtractedTextTest extends AbstractIntegrationTest {

    private static final UserDefinedFunction TEXT_EXTRACTOR = TextExtractor.build();

    @Test
    void test() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("content")));

        assertEquals(2, dataset.count(), "Expecting 2 files from the directory");
        assertEquals(9, dataset.collectAsList().get(0).size(), "Expecting the 8 standard columns for representing a " +
            "column, plus the 'extractedText' column.");

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .mode(SaveMode.Append)
            .save();

        TextDocumentManager mgr = getDatabaseClient().newTextDocumentManager();
        String text = mgr.read("/extract-test/hello-world.docx-extracted-text.txt", new StringHandle()).get();
        assertTrue(text.startsWith("Hello world"), "Unexpected text: " + text);

        text = mgr.read("/extract-test/marklogic-getting-started.pdf-extracted-text.txt", new StringHandle()).get();
        assertTrue(text.contains("MarkLogic Server Table of Contents"), "Unexpected text: " + text);
    }

    @Test
    void invalidColumn() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", TEXT_EXTRACTOR.apply(new Column("uri")));

        SparkException ex = assertThrows(SparkException.class, () -> dataset.collectAsList());
        assertTrue(ex.getMessage().contains("Text extraction UDF must be run against a column containing non-null byte arrays."),
            "Unexpected error: " + ex.getMessage());
    }

}
