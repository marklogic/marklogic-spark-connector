package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadRdfZipFilesTest extends AbstractIntegrationTest {

    @Test
    void twoRdfFilesInZip() {
        List<Row> rows = startRead()
            .load("src/test/resources/rdf/two-rdf-files.zip")
            .collectAsList();

        assertEquals(40, rows.size(), "Expecting 32 triples from the englishlocale.ttl file in the zip " +
            "and 8 triples from the mini-taxonomy.xml file.");

        Map<String, Integer> subjectCounts = getSubjectCounts(rows);
        assertEquals(4, subjectCounts.get("http://marklogicsparql.com/id#1111"));
        assertEquals(8, subjectCounts.get("http://vocabulary.worldbank.org/taxonomy/451"));
    }

    @Test
    void zipHasEmptyRdfFile() {
        List<Row> rows = startRead()
            .load("src/test/resources/rdf/has-empty-entry.zip")
            .collectAsList();

        assertEquals(32, rows.size(), "Expecting 32 triples from englishlocale.ttl and zero triples from " +
            "empty-taxonomy.xml. The fact that empty-taxonomy.xml is a valid XML file but has no triples should not " +
            "result in any error.");
    }

    @Test
    void eachRdfFileTypeInZip() {
        List<Row> rows = startRead()
            .load("src/test/resources/rdf/each-rdf-file-type.zip")
            .collectAsList();

        assertEquals(105, rows.size(), "Expecting the following counts: 32 from englishlocale.ttl; 8 from " +
            "mini-taxonomy.xml; 12 from semantics.json; 25 from semantics.n3; 8 from semantics.nt; 16 from " +
            "three-quads.trig; and 4 from semantics.nq.");

        Map<String, Integer> subjectCounts = getSubjectCounts(rows);
        assertEquals(4, subjectCounts.get("http://marklogicsparql.com/id#1111"),
            "Verifies that englishlocale.ttl was read correctly.");
        assertEquals(8, subjectCounts.get("http://vocabulary.worldbank.org/taxonomy/451"),
            "Verifies that mini-taxonomy.xml was read correctly.");
        assertEquals(12, subjectCounts.get("http://jondoe.example.org/#me"),
            "Verifies that semantics.json was read correctly.");
        assertEquals(4, subjectCounts.get("http://www.w3.org/2001/sw/RDFCore/ntriples/"),
            "Verifies that semantics.nt was read correctly.");
        assertEquals(11, subjectCounts.get("http://purl.org/dc/elements/1.1/"),
            "Verifies that semantics.n3 was read correctly.");
        assertEquals(1, subjectCounts.get("http://dbpedia.org/resource/Animal_Farm"),
            "Verifies that semantics.nq was read correctly.");
        assertEquals(6, subjectCounts.get("http://www.example.org/exampleDocument#Monica"),
            "Verifies that three-quads.trig was read correctly.");
    }

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .option(Options.READ_FILES_COMPRESSION, "zip");
    }

    private Map<String, Integer> getSubjectCounts(List<Row> rows) {
        Map<String, Integer> subjectCounts = new HashMap<>();
        rows.forEach(row -> {
            String subject = row.getString(0);
            if (subjectCounts.containsKey(subject)) {
                subjectCounts.put(subject, subjectCounts.get(subject) + 1);
            } else {
                subjectCounts.put(subject, 1);
            }
        });
        return subjectCounts;
    }
}
