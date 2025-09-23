/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRdfFilesTest extends AbstractIntegrationTest {

    @ParameterizedTest
    @CsvSource({
        ",ttl", // TTL is the default format.
        "nt,nt",
        "ntriples,nt",
        "rdfthrift,thrift",
        "rdfproto,binpb"
    })
    void tripleFormats(String format, String fileExtension, @TempDir Path tempDir) {
        writeExampleGraphToFiles(tempDir, format, fileExtension);

        List<Row> rows = readRdfFiles(tempDir);
        verifyGraphIsNullForEachRow(rows);
        verifyDebtRowHasLang(rows);
        verifyCreatorRowHasDatatype(rows);
    }

    /**
     * TriX - https://en.wikipedia.org/wiki/TriX_(serialization_format) - is treated as a quads format as it can
     * support a graph at /trix/graph/uri in each TriX XML document.
     * <p>
     * Also, Jena is reporting errors with a message of "Unexpected attribute : :datatype at typedLiteral", but that
     * does not seem to be causing any issue. Perhaps a bug in Jena, as TriX supports a 'datatype' attribute on a
     * 'typedLiteral' element.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "trix",
        "trig",
        "nq"
    })
    void trix(String format, @TempDir Path tempDir) {
        writeExampleGraphToFiles(tempDir, format);

        List<Row> rows = readRdfFiles(tempDir);
        verifyEachRowHasGraph(rows);
        verifyDebtRowHasLang(rows);
        verifyCreatorRowHasDatatype(rows);
    }

    @Test
    void nquads(@TempDir Path tempDir) {
        writeExampleGraphToFiles(tempDir, "nquads", "nq");

        List<Row> rows = readRdfFiles(tempDir);
        verifyEachRowHasGraph(rows);
        verifyDebtRowHasLang(rows);
        verifyCreatorRowHasDatatype(rows);
    }

    @Test
    void tripleFormatWithGraphOverride(@TempDir Path tempDir) {
        writeExampleGraphToFiles(tempDir, null, "ttl", "this-should-be-ignored");

        // If the format doesn't support a graph, then a user-defined graph should be ignored without any error
        // thrown.
        List<Row> rows = readRdfFiles(tempDir);
        verifyGraphIsNullForEachRow(rows);
    }

    @Test
    void quadsFormatWithGraphOverride(@TempDir Path tempDir) {
        writeExampleGraphToFiles(tempDir, "nquads", "nq", "use-this-graph");

        List<Row> rows = readRdfFiles(tempDir);
        verifyEachRowHasGraph(rows, "use-this-graph");
    }

    @Test
    void noTriplesFound(@TempDir Path tempDir) {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_URIS, "/not-a-document.json")
            .load();

        assertEquals(0, dataset.count());

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(0, tempDir.toFile().listFiles().length, "No files should have been written since no triples " +
            "were found.");
    }


    private void writeExampleGraphToFiles(Path tempDir, String format) {
        writeExampleGraphToFiles(tempDir, format, format);
    }

    private void writeExampleGraphToFiles(Path tempDir, String format, String expectedFileExtension) {
        writeExampleGraphToFiles(tempDir, format, expectedFileExtension, null);
    }

    private void writeExampleGraphToFiles(Path tempDir, String format, String expectedFileExtension, String graphOverride) {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .load()
            .repartition(2) // Force 2 files to be written, just so we can ensure more than 1 works.
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, format)
            .option(Options.WRITE_RDF_FILES_GRAPH, graphOverride)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File[] files = tempDir.toFile().listFiles();
        assertEquals(2, files.length);

        assertTrue(files[0].getName().endsWith(expectedFileExtension), "Unexpected filename: " + files[0].getName());
        assertTrue(files[1].getName().endsWith(expectedFileExtension), "Unexpected filename: " + files[1].getName());
    }

    private List<Row> readRdfFiles(Path tempDir) {
        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();
        assertEquals(8, rows.size());
        return rows;
    }

    private void verifyEachRowHasGraph(List<Row> rows) {
        verifyEachRowHasGraph(rows, "http://example.org/graph");
    }

    private void verifyEachRowHasGraph(List<Row> rows, String graph) {
        rows.forEach(row -> assertEquals(graph, row.getString(5), "Since the trig format " +
            "supports quads, the graph should have been included in the written files and then read back by our " +
            "connector."));
    }

    private void verifyGraphIsNullForEachRow(List<Row> rows) {
        rows.forEach(row -> assertTrue(row.isNullAt(5), "Each row should have a null graph since it was written " +
            "to a format that only supports triples and not quads."));
    }

    private void verifyDebtRowHasLang(List<Row> rows) {
        Row debtRow = rows.stream().filter(row -> "Debt Management".equals(row.getString(2))).findFirst().get();
        assertEquals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", debtRow.getString(3), "For a " +
            "langString, the datatype can't be included in the file as Jena only lets us write either a typed literal " +
            "or a lang literal. But when reading in the rows, we know that if a 'lang' value exists, then the " +
            "datatype must be a langString.");
        assertEquals("en", debtRow.getString(4));
    }

    private void verifyCreatorRowHasDatatype(List<Row> rows) {
        Row creatorRow = rows.stream().filter(row -> "wb".equals(row.getString(2))).findFirst().get();
        assertEquals("http://www.w3.org/2001/XMLSchema#string", creatorRow.getString(3), "Verifying that the " +
            "datatype is set correctly based on what is written to the file.");
    }
}
