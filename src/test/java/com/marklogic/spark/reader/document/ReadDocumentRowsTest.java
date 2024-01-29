package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadDocumentRowsTest extends AbstractIntegrationTest {

    @Test
    void readByCollection() throws Exception {
        Dataset<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load();

        assertEquals(15, rows.count());

        Row row = rows.filter("URI = '/author/author1.json'").collectAsList().get(0);
        assertEquals("/author/author1.json", row.getString(0));
        assertEquals("JSON", row.getString(2));
        JsonNode doc = new ObjectMapper().readTree((byte[]) row.get(1));
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
        Dataset<Row> rows = startRead()
            // Query type defaults to "string", so no need to specify it.
            .option(Options.READ_DOCUMENTS_QUERY, "Vivianne OR Moria")
            .load();

        List<String> uris = rows.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
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

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }
}
