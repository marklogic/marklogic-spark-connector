/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrettyPrintFilesTest extends AbstractIntegrationTest {

    @Test
    void xmlAndJson(@TempDir Path tempDir) throws IOException {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "pretty-print")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_PRETTY_PRINT, "true")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File dir = new File(tempDir.toFile(), "pretty-print");
        String doc1 = FileUtils.readFileToString(new File(dir, "doc1.xml"), "UTF-8");
        assertEquals("""
                <root>
                    <hello>world</hello>
                </root>
                """, doc1,
            "Pretty-printing should result in the XML declaration being omitted and child elements being " +
                "indented with a default indent of 4. This mirrors how XML is pretty-printed by MLCP.");

        String doc2 = FileUtils.readFileToString(new File(dir, "doc2.json"), "UTF-8");
        assertEquals("""
            {
              "hello" : "world"
            }""", doc2, "The JSON should be pretty-printed.");
    }

    @Test
    void zipWithXmlAndJson(@TempDir Path tempDir) {
        SparkSession session = newSparkSession();

        session.read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "pretty-print")
            .load()
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_PRETTY_PRINT, "true")
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File[] files = tempDir.toFile().listFiles();
        assertEquals(1, files.length, "Expecting a single zip file due to the repartition call.");

        // Use the connector to read the entries back in, which is a convenient way of checking their content.
        List<Row> rows = session.read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(files[0].getAbsolutePath())
            .orderBy(new Column("uri"))
            .collectAsList();

        String xml = new String((byte[]) rows.get(0).get(1));
        assertEquals("""
            <root>
                <hello>world</hello>
            </root>
            """, xml);

        String json = new String((byte[]) rows.get(1).get(1));
        assertEquals("""
            {
              "hello" : "world"
            }""", json);
    }

    @Test
    void notPrettyPrinted(@TempDir Path tempDir) throws IOException {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "pretty-print")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File dir = new File(tempDir.toFile(), "pretty-print");
        String doc1 = FileUtils.readFileToString(new File(dir, "doc1.xml"), "UTF-8");
        assertEquals("""
            <?xml version="1.0" encoding="UTF-8"?>
            <root><hello>world</hello></root>""", doc1);

        String doc2 = FileUtils.readFileToString(new File(dir, "doc2.json"), "UTF-8");
        assertEquals("{\"hello\":\"world\"}", doc2);
    }

    @Test
    void notSupportedWhenStreaming(@TempDir Path tempDir) throws Exception {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "pretty-print")
            .load()
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .option(Options.WRITE_FILES_PRETTY_PRINT, "true")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File dir = new File(tempDir.toFile(), "pretty-print");
        String doc1 = FileUtils.readFileToString(new File(dir, "doc1.xml"), "UTF-8");
        assertTrue(doc1.contains("<root><hello>world</hello></root>"),
            "pretty-printed is not supported when streaming documents, as pretty-printing requires reading the " +
                "document into memory, which conflicts with streaming. So the XML doc should be on a single line. " +
                "Actual doc: " + doc1);
    }
}
