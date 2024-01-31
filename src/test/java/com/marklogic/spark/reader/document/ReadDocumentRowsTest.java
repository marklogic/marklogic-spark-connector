package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ReadDocumentRowsTest extends AbstractIntegrationTest {

    @Test
    void readByCollection() {
        Dataset<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load();

        assertEquals(15, rows.count());

        Row row = rows.filter("URI = '/author/author1.json'").collectAsList().get(0);
        assertEquals("/author/author1.json", row.getString(0));
        assertEquals("JSON", row.getString(2));
        JsonNode doc = readJsonContent(row);
        // Verify just a couple fields to ensure the JSON object is correct.
        assertEquals(4, doc.get("CitationID").asInt());
        assertEquals("Vivianne", doc.get("ForeName").asText());
    }

    @Test
    void noDocsInCollection() {
        long count = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "some-collection-with-no-documents")
            .load()
            .count();
        assertEquals(0, count);
    }

    @Test
    void stringQuery() {
        List<Row> rows = startRead()
            // Query type defaults to "string", so no need to specify it.
            .option(Options.READ_DOCUMENTS_QUERY, "Vivianne OR Moria")
            .load()
            .collectAsList();

        List<String> uris = getUrisFromRows(rows);
        assertEquals(2, uris.size());
        assertTrue(uris.contains("/author/author1.json"));
        assertTrue(uris.contains("/author/author11.json"));
    }

    @Test
    void structuredQueryXML() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "strucTURED")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("/author/author1.json", rows.get(0).getString(0));
    }

    @Test
    void structuredQueryWithCollection() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "structured")
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void structuredQueryWithNonMatchingCollection() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "STRUCTURED")
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "some-other-collection")
            .load()
            .collectAsList();

        assertEquals(0, rows.size());
    }

    @Test
    void structuredQueryJSON() {
        String query = "{ \"query\": { \"queries\": [{ \"term-query\": { \"text\": [ \"Moria\" ] } }] } }";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "STRUCTured")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("/author/author11.json", rows.get(0).getString(0));
    }

    @Test
    void serializedCTSQueryXML() {
        String query = "<cts:word-query xmlns:cts='http://marklogic.com/cts'>" +
            "<cts:text xml:lang='en'>Vivianne</cts:text>" +
            "</cts:word-query>";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "serialized_cts")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void serializedCTSQueryJSON() {
        String query = "{ \"query\": { \"queries\": [{ \"term-query\": { \"text\": [ \"Vivianne\" ] } }] } }";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "serialized_cts")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void combinedQueryXML() {
        String query = "<search xmlns='http://marklogic.com/appservices/search'>" +
            "<cts:word-query xmlns:cts='http://marklogic.com/cts'><cts:text xml:lang='en'>Vivianne</cts:text></cts:word-query>" +
            "<options/></search>";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "combined")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void combinedQueryJSON() {
        String query = "{ \"search\": { \"query\": { \"query\": { \"queries\": [{\"term-query\":{\"text\":[\"Vivianne\"]}}] } } } }";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "combined")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void invalidQueryType() {
        Dataset<Row> dataset = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, "<test/>")
            .option(Options.READ_DOCUMENTS_QUERY_TYPE, "not-valid-type")
            .load();

        SparkException sparkException = assertThrows(SparkException.class, () -> dataset.collectAsList());
        IllegalArgumentException ex = (IllegalArgumentException) sparkException.getCause();
        assertEquals("Invalid query type: not-valid-type; must be one of [STRING, STRUCTURED, SERIALIZED_CTS, COMBINED]",
            ex.getMessage());
    }

    @Test
    void directory() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/author/")
            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Should retrieve all 15 documents in the '/author/' directory.");
    }

    @Test
    void directoryWithNoDocuments() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/has-no-documents/")
            .load()
            .collectAsList();

        assertEquals(0, rows.size());
    }

    @Test
    void withSearchOptions() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, "citation_id:3")
            .option(Options.READ_DOCUMENTS_OPTIONS_NAME, "test-options")
            .load()
            .collectAsList();

        assertEquals(4, rows.size(), "4 authors have a CitationID of 3");
        List<String> uris = getUrisFromRows(rows);
        assertTrue(uris.contains("/author/author6.json"));
        assertTrue(uris.contains("/author/author10.json"));
        assertTrue(uris.contains("/author/author11.json"));
        assertTrue(uris.contains("/author/author15.json"));
    }

    /**
     * See BuildSearchQueryTest for additional tests for constructing a server transform.
     */
    @Test
    void withTransform() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, "Vivianne")
            .option(Options.READ_DOCUMENTS_TRANSFORM, "withParams")
            .option(Options.READ_DOCUMENTS_TRANSFORM_PARAMS, "param1;value,1;param2;value2")
            .option(Options.READ_DOCUMENTS_TRANSFORM_PARAMS_DELIMITER, ";")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        JsonNode doc = readJsonContent(rows.get(0));

        String message = "Doc should have been transformed via withParams server transform: " + doc.toPrettyString();
        assertTrue(doc.has("content"), message);
        assertTrue(doc.has("params"), message);
        assertTrue(doc.has("context"), message);

        JsonNode params = doc.get("params");
        assertEquals("value,1", params.get("param1").asText());
        assertEquals("value2", params.get("param2").asText());
    }

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

    private List<String> getUrisFromRows(List<Row> rows) {
        return rows.stream().map(row -> row.getString(0)).collect(Collectors.toList());
    }

    private JsonNode readJsonContent(Row row) {
        try {
            return objectMapper.readTree((byte[]) row.get(1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
