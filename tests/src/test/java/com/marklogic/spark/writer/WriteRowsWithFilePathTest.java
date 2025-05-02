/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRowsWithFilePathTest extends AbstractWriteTest {

    /**
     * Intended to allow for Flux to optionally use the filename for an initial URI. Relevant any time we use Flux with
     * a Spark data source that produces arbitrary data rows.
     */
    @Test
    void test() {
        newSparkSession().read()
            .option("header", true)
            .format("csv")
            .csv("src/test/resources/data.csv")
            .withColumn("marklogic_spark_file_path", new Column("_metadata.file_path"))
            .limit(10)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "some-files")
            .option(Options.WRITE_URI_REPLACE, ".*/src/test,'/test'")
            .mode(SaveMode.Append)
            .save();

        List<String> uris = getUrisInCollection("some-files", 10);
        uris.forEach(uri -> {
            assertTrue(uri.startsWith("/test/resources/data.csv/"), "When a column named 'marklogic_spark_file_path' is passed " +
                "to the connector for writing arbitrary rows, it will be used to construct an initial URI that " +
                "also has a UUID in it. This is useful for the somewhat rare use case of wanting the physical file " +
                "path to be a part of the URI (as opposed to using a URI template). Actual URI: " + uri);

            JsonNode doc = readJsonDocument(uri);
            assertEquals(2, doc.size(), "The marklogic_spark_file_path column should not have been used when " +
                "constructing the JSON document. This includes when ignoreNullFields is set to false. We still want " +
                "the column removed as the column is an implementation detail that should not be exposed to the user. " +
                "If we ever want the file path to be included in the document, we'll add an explicit feature for that.");
            assertTrue(doc.has("docNum"));
            assertTrue(doc.has("docName"));
        });
    }
}
