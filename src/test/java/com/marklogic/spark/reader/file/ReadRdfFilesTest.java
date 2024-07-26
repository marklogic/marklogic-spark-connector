package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadRdfFilesTest extends AbstractIntegrationTest {

    @Test
    void rdfXml() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/mini-taxonomy.xml");

        List<Row> rows = dataset.collectAsList();
        assertEquals(8, rows.size(), "Expecting 8 triples, as there are 8 child elements in the " +
            "single rdf:Description element in the test file.");

        // Verify a few triples to make sure things look good.
        final String subject = "http://vocabulary.worldbank.org/taxonomy/451";
        verifyRow(rows.get(0), subject, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2004/02/skos/core#Concept");
        verifyRow(rows.get(1), subject, "http://purl.org/dc/terms/creator", "wb", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(4), subject, "http://www.w3.org/2004/02/skos/core#prefLabel", "Debt Management",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "en");
    }

    @Test
    void emptyRdfXml() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/empty-taxonomy.xml");
        assertEquals(0, dataset.count(), "Verifying that no error is thrown if an RDF file is valid but simply " +
            "has no triples in it.");
    }

    /**
     * Verifies that blank nodes are generated in the same manner as with MLCP.
     */
    @Test
    void blankNodes() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/blank-nodes.xml");

        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());

        final String subject = "http://example.org/web-data";
        verifyRow(rows.get(0), subject, "http://example.org/data#title", "Web Data", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(1), subject, "http://example.org/data#professor", "BLANK");
        verifyRow(rows.get(2), "BLANK", "http://example.org/data#fullName", "Alice Carol", "http://www.w3.org/2001/XMLSchema#string", null);
        verifyRow(rows.get(3), "BLANK", "http://example.org/data#homePage", "http://example.net/alice-carol");
    }

    @Test
    void turtleTriples() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/englishlocale.ttl");
        List<Row> rows = dataset.collectAsList();
        assertEquals(32, rows.size());

        // Verify a few rows as a sanity check.
        final String subject = "http://marklogicsparql.com/id#1111";
        verifyRow(rows.get(0), subject, "http://marklogicsparql.com/addressbook#firstName", "John",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
        verifyRow(rows.get(1), subject, "http://marklogicsparql.com/addressbook#lastName", "Snelson",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
        verifyRow(rows.get(31), "http://marklogicsparql.com/id#8888", "http://marklogicsparql.com/addressbook#email",
            "Lihan.Wang@gmail.com", "http://www.w3.org/2001/XMLSchema#string", null, null);
    }

    @Test
    void unrecognizedExtension() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/turtle-triples.txt");
        ConnectorException ex = assertThrowsConnectorException(() -> dataset.show());
        assertTrue(
            ex.getMessage().contains("RDF syntax is not supported or the file extension is not recognized."),
            "We rely on Jena to identify a file based on its extension (with the exception of RDF JSON; Jena for " +
                "some reason does not recognize .json as an extension). If the user provides a file with an " +
                "extension that Jena does not recognize, we expect a friendly error that doesn't contain the " +
                "Jena message of '(.lang or .base required)', which a user won't understand and can't do " +
                "anything about. Actual error: " + ex.getMessage()
        );
    }

    @Test
    void rdfJson() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/semantics.json");
        List<Row> rows = dataset.collectAsList();
        assertEquals(12, rows.size());

        // Verify a few rows as a sanity check.
        final String subject = "http://jondoe.example.org/#me";
        verifyRow(rows.get(0), subject, "http://www.w3.org/2000/01/rdf-schema#type", "http://xmlns.com/foaf/0.1/Person");
        verifyRow(rows.get(1), subject, "http://xmlns.com/foaf/0.1/name", "Jon",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
        verifyRow(rows.get(11), subject, "http://www.w3.org/2006/vcard/ns#tel", "+49-12-3546789",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
    }

    @Test
    void n3Triples() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/semantics.n3");
        List<Row> rows = dataset.collectAsList();
        assertEquals(25, rows.size());

        // Verify a few rows as a sanity check.
        verifyRow(rows.get(0), "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#List");
        verifyRow(rows.get(14), "http://purl.org/dc/elements/1.1/", "http://purl.org/dc/elements/1.1/description",
            "The Dublin Core Element Set v1.1 namespace provides URIs for the Dublin Core Elements v1.1. Entries are declared using RDF Schema language to support RDF applications.",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "en-US", null);
        verifyRow(rows.get(24), "http://purl.org/dc/elements/1.1/", "http://purl.org/dc/terms/modified", "2003-03-24",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
    }

    @Test
    void ntriples() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/semantics.nt");
        List<Row> rows = dataset.collectAsList();
        assertEquals(8, rows.size());

        // Verify a few rows as a sanity check.
        verifyRow(rows.get(0), "http://www.w3.org/2001/sw/RDFCore/ntriples/", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://xmlns.com/foaf/0.1/Document");
        verifyRow(rows.get(1), "http://www.w3.org/2001/sw/RDFCore/ntriples/", "http://purl.org/dc/terms/title",
            "N-Triples", "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "en-US", null);
        verifyRow(rows.get(7), "BLANK", "http://xmlns.com/foaf/0.1/name", "Dave Beckett",
            "http://www.w3.org/2001/XMLSchema#string", null, null);
    }

    @Test
    void trigQuads() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/three-quads.trig");
        List<Row> rows = dataset.collectAsList();
        assertEquals(16, rows.size());

        verifyRow(rows.get(0), "http://www.example.org/exampleDocument#Monica", "http://www.example.org/vocabulary#name",
            "Monica Murphy", "http://www.w3.org/2001/XMLSchema#string", null, "http://www.example.org/exampleDocument#G1");

        verifyRow(rows.get(4), "http://www.example.org/exampleDocument#Monica", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.example.org/vocabulary#Person", null, null, "http://www.example.org/exampleDocument#G2");

        verifyRow(rows.get(6), "http://www.example.org/exampleDocument#G1", "http://www.w3.org/2004/03/trix/swp-1/assertedBy",
            "BLANK", null, null, "http://www.example.org/exampleDocument#G3");

        // Verifies that Jena uses urn:x-arq:DefaultGraphNode as the default graph when a graph is not specified
        // in a quads file.
        verifyRow(rows.get(15), "http://www.example.org/exampleDocument#Default", "http://www.example.org/vocabulary#graphname",
            "Default", "http://www.w3.org/2001/XMLSchema#string", null, "urn:x-arq:DefaultGraphNode");
    }

    @Test
    void nquads() {
        Dataset<Row> dataset = startRead().load("src/test/resources/rdf/semantics.nq");

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

    @Test
    void dontAbortOnTriplesFailure() {
        List<Row> rows = startRead()
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/data.csv", "src/test/resources/rdf/mini-taxonomy.xml")
            .collectAsList();

        assertEquals(8, rows.size(), "The error from data.csv should be caught and logged, and then the 8 triples " +
            "from mini-taxonomy.xml should be returned.");
    }

    @Test
    void dontAbortOnQuadsFailure() {
        List<Row> rows = startRead()
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/rdf/bad-quads.trig", "src/test/resources/rdf/three-quads.trig")
            .collectAsList();

        assertEquals(20, rows.size(), "Should have the 20 triples from three-quads.trig, and the error from " +
            "bad-quads.trig should have been caught and logged at the WARN level.");
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

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf");
    }
}
