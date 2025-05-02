/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRdfGzipFilesTest extends AbstractIntegrationTest {

    @Test
    void gzip(@TempDir Path tempDir) {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .load();

        assertEquals(8, dataset.count());

        dataset.repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, "nt")
            .option(Options.WRITE_FILES_COMPRESSION, "gzip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File[] files = tempDir.toFile().listFiles();
        assertEquals(1, files.length, "Expecting 1 gzip file due to repartition=1 producing one writer.");
        assertTrue(files[0].getName().endsWith(".nt.gz"), "Unexpected filename: " + files[0].getName());

        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        assertEquals(8, rows.size(), "Expecting the 8 rows originally read from MarkLogic to be read " +
            "from the single gzipped file.");
    }

    @Test
    void zipIsntValidChoice() {
        DataFrameWriter writer = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, "nt")
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save("."));
        assertEquals("Unsupported compression value; only 'gzip' is supported: zip", ex.getMessage());
    }
}
