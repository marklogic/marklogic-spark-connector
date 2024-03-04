package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadDocumentRowsByUrisTest extends AbstractIntegrationTest {

    @Test
    void readByUris() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json\n" +
                "/author/author12.json\n" +
                "/author/author3.json")
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/author/")
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .collectAsList();

        assertEquals(3, rows.size());
    }

    @Test
    void urisWithStringQuery() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json\n" +
                "/author/author12.json\n" +
                "/author/author3.json")
            .option(Options.READ_DOCUMENTS_STRING_QUERY, "Wooles")
            .load()
            .collectAsList();

        assertEquals(1, rows.size(), "The string query should result in only author1 being returned.");
        assertEquals("/author/author1.json", rows.get(0).getString(0));
    }

    @Test
    void urisWithOtherQuery() {
        final String query = "{\"ctsquery\": {\"wordQuery\": {\"text\": \"Vivianne\"}}}";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_URIS, "/author/author12.json")
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .load()
            .collectAsList();

        assertEquals(1, rows.size(), "When a query is specified along with a list of URIs, the connector should log " +
            "a warning about ignoring the query and just using the list of URIs. This is due to us not yet having a " +
            "reliable way of combining a directory query with a structured query, a serialized CTS query, and a " +
            "combined query, particularly when those can be in XML or JSON.");
        assertEquals("/author/author12.json", rows.get(0).getString(0));
    }

    @Test
    void urisWithWrongDirectory() {
        long count = startRead()
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json")
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/wrong/")
            .load()
            .count();

        assertEquals(0, count, "This verifies that the directory impacts the list of URIs.");
    }

    @Test
    void urisWithWrongCollection() {
        long count = startRead()
            .option(Options.READ_DOCUMENTS_URIS, "/author/author1.json")
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "wrong")
            .load()
            .count();

        assertEquals(0, count, "This verifies that the collection impacts the list of URIs.");
    }

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

}
