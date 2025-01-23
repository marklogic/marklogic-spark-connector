/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.udf.tika.TextExtractor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteExtractedTextTest extends AbstractIntegrationTest {

    @Test
    void test() {
        final UserDefinedFunction textExtractor = TextExtractor.build();

        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load("src/test/resources/extraction-files")
            .withColumn("extractedText", textExtractor.apply(new Column("content")))
            .write().format(CONNECTOR_IDENTIFIER)
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

}
