package com.marklogic.spark.reader.triples;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadAndWriteTriplesTest extends AbstractIntegrationTest {

    private static final String OUTPUT_PATH = "build/triples";

    @BeforeEach
    void beforeEach() {
        // Using a local directory for easy manual inspection of data for now.
        File dir = new File(OUTPUT_PATH);
        FileUtil.fullyDeleteContents(dir);
        dir.mkdirs();
    }

    @Test
    void defaultFormat() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://marklogic.com/semantics#default-graph")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load();
        assertEquals(1000, dataset.count());

        dataset.repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save(OUTPUT_PATH);

        File[] rdfFiles = new File(OUTPUT_PATH).listFiles();
        assertEquals(1, rdfFiles.length);
        String filename = rdfFiles[0].getName();
        assertTrue(filename.endsWith("-0.ttl"), "Expecting file to end with partition ID and then the " +
            "default format suffix of .ttl: " + filename);

        dataset = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load(OUTPUT_PATH);
        assertEquals(1000, dataset.count());
    }

    @Test
    void withGraph() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://marklogic.com/semantics#default-graph")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, "nq")
            .option(Options.WRITE_RDF_FILES_GRAPH, "org:example:graph")
            .mode(SaveMode.Append)
            .save(OUTPUT_PATH);

        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load(OUTPUT_PATH)
            .collectAsList();

        assertEquals(1000, rows.size());
        rows.forEach(row -> assertEquals("org:example:graph", row.getString(5)));
    }

    @Test
    void badFormatWithGraph() {
        DataFrameWriter writer = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://marklogic.com/semantics#default-graph")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, "ttl")
            .option(Options.WRITE_RDF_FILES_GRAPH, "org:example:graph")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrows(ConnectorException.class, () -> writer.save(OUTPUT_PATH));
        assertEquals(
            "If specifying a graph when writing RDF data, the format must be either 'nq' or 'trig'; format: ttl.",
            ex.getMessage()
        );
    }
}
