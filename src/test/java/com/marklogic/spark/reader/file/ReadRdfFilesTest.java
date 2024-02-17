package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadRdfFilesTest extends AbstractIntegrationTest {

    @Test
    void rdfXml() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/mini-taxonomy.xml");

        List<Row> rows = dataset.collectAsList();
        assertEquals(8, rows.size(), "Expecting 8 triples, as there are 8 child elements in the " +
            "single rdf:Description element in the test file.");

        // Verify a few triples to make sure things look good.
        final String subject = "http://vocabulary.worldbank.org/taxonomy/451";
        verifyRow(rows.get(0), subject, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2004/02/skos/core#Concept");
        verifyRow(rows.get(1), subject, "http://purl.org/dc/terms/creator", "wb", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(4), subject, "http://www.w3.org/2004/02/skos/core#prefLabel", "Debt Management", null, "en");
    }

    /**
     * Verifies that blank nodes are generated in the same manner as with MLCP.
     */
    @Test
    void blankNodes() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/blank-nodes.xml");

        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());

        final String subject = "http://example.org/web-data";
        verifyRow(rows.get(0), subject, "http://example.org/data#title", "Web Data", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(1), subject, "http://example.org/data#professor", "BLANK");
        verifyRow(rows.get(2), "BLANK", "http://example.org/data#fullName", "Alice Carol", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(3), "BLANK", "http://example.org/data#homePage", "http://example.net/alice-carol");
    }

    @Test
    void trigQuads() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/three-quads.trig");

        List<Row> rows = dataset.collectAsList();
        assertEquals(16, rows.size());

        verifyRow(rows.get(0), "http://www.example.org/exampleDocument#Monica", "http://www.example.org/vocabulary#name",
            "Monica Murphy", "http://www.w3.org/2001/XMLSchema#string", null, "http://www.example.org/exampleDocument#G1");

        verifyRow(rows.get(4), "http://www.example.org/exampleDocument#Monica", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.example.org/vocabulary#Person", null, null, "http://www.example.org/exampleDocument#G2");

        verifyRow(rows.get(6), "http://www.example.org/exampleDocument#G1", "http://www.w3.org/2004/03/trix/swp-1/assertedBy",
            "BLANK", null, null, "http://www.example.org/exampleDocument#G3");
    }

    @Test
    void nquads() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/semantics.nq");

        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());

        verifyRow(rows.get(0), "http://dbpedia.org/resource/Autism", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://dbpedia.org/ontology/Disease", null, null, "http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9");

        // MLCP converts urn:x-arq:DefaultGraphNode into MarkLogic's default graph.
        verifyRow(rows.get(1), "http://dbpedia.org/resource/Autism", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://dbpedia.org/ontology/Disease1", null, null, "urn:x-arq:DefaultGraphNode");

        verifyRow(rows.get(2), "http://dbpedia.org/resource/Animal_Farm", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://schema.org/CreativeWork", null, null, "http://en.wikipedia.org/wiki/Animal_Farm?oldid=494597186#absolute-line=5");

        verifyRow(rows.get(3), "http://dbpedia.org/resource/Aristotle", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://dbpedia.org/ontology/Agent", null, null, "http://en.wikipedia.org/wiki/Aristotle?oldid=494147695#absolute-line=4");
    }

    private void verifyRow(Row row, String subject, String predicate, String object) {
        verifyRow(row, subject, predicate, object, null, null);
    }

    private void verifyRow(Row row, String subject, String predicate, String object, String datatype, String lang) {
        verifyRow(row, subject, predicate, object, datatype, lang, null);
    }

    private void verifyRow(Row row, String subject, String predicate, String object, String datatype, String lang, String graph) {
        assertEqualsOrBlank(subject, row.getString(0));
        assertEquals(predicate, row.getString(1));
        assertEqualsOrBlank(object, row.getString(2));
        assertEquals(datatype, row.get(3));
        assertEquals(lang, row.getString(4));
        assertEquals(graph, row.getString(5));
    }

    private void assertEqualsOrBlank(String expectedValue, String actualValue) {
        if ("BLANK".equals(expectedValue)) {
            assertTrue(actualValue.startsWith("http://marklogic.com/semantics/blank/"),
                "We are reusing copy/pasted code from MLCP for generating a 'blank' value, which is expected to end with " +
                    "a random hex value. It is not known why this isn't just a Java-generated UUID; we're simply reusing " +
                    "the code because it's what MLCP does. Actual value: " + actualValue);
        } else {
            assertEquals(expectedValue, actualValue);
        }
    }
}
