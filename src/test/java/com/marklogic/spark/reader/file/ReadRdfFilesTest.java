package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

        dataset.show(10, 0, true);
        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());

        verifyRow(rows.get(0), "http://example.org/web-data", "http://example.org/data#title", "Web Data",
            "http://www.w3.org/2001/XMLSchema#string", null);
        
        assertBlankValue(rows.get(1).getString(2));
        assertBlankValue(rows.get(2).getString(0));
        assertBlankValue(rows.get(3).getString(0));
    }

    private void verifyRow(Row row, String subject, String predicate, String object) {
        verifyRow(row, subject, predicate, object, null, null);
    }

    private void verifyRow(Row row, String subject, String predicate, String object, String datatype, String lang) {
        assertEquals(subject, row.getString(0));
        assertEquals(predicate, row.getString(1));
        assertEquals(object, row.getString(2));
        assertEquals(datatype, row.get(3));
        assertEquals(lang, row.getString(4));
        assertNull(row.get(5), "The graph is expected to be null since these are triples and not quads.");
    }

    private void assertBlankValue(String value) {
        assertTrue(value.startsWith("http://marklogic.com/semantics/blank/"),
            "We are reusing copy/pasted code from MLCP for generating a 'blank' value, which is expected to end with " +
                "a random hex value. It is not known why this isn't just a Java-generated UUID; we're simply reusing " +
                "the code because it's what MLCP does. Actual value: " + value);
    }
}
