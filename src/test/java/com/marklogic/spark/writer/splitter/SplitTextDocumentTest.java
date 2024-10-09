/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

class SplitTextDocumentTest extends AbstractIntegrationTest {

    @Test
    void test() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/marklogic-docs/java-client-intro.txt")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_PREFIX, "/test")
            .mode(SaveMode.Append)
            .save();

        final String chunksUri = "/test/marklogic-docs/java-client-intro.txt-chunks.xml";

        XmlNode doc = readXmlDocument(chunksUri);
        doc.assertElementValue("/root/source-uri", "/test/marklogic-docs/java-client-intro.txt");
        doc.assertElementCount("Expecting 2 chunks based on the default max chunk size of 1000",
            "/root/chunks/chunk", 2);

        PermissionsTester tester = readDocumentPermissions(chunksUri);
        tester.assertUpdatePermissionExists("This is just a temporary permission until we allow the URI and " +
            "metadata for chunk documents to be configurable", "spark-user-role");
        tester.assertReadPermissionExists("spark-user-role");
    }
}
