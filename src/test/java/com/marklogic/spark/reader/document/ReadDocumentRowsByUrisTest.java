package com.marklogic.spark.reader.document;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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

    @Test
    void nonUsAsciiUri() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/encoding/太田佳伸のＸＭＬファイル.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_REPLACE, ".*encoding,''")
            .mode(SaveMode.Append)
            .save();

        final String expectedUri = "/太田佳伸のＸＭＬファイル.xml";
        XmlNode doc = readXmlDocument(expectedUri);
        doc.assertElementValue("/root/filename", "太田佳伸のＸＭＬファイル");

        Dataset<Row> dataset = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, expectedUri)
            .load();

        assertEquals(1, dataset.count());
        Row row = dataset.collectAsList().get(0);
        assertEquals(expectedUri, row.getString(0),
            "As of 7.0.0, the Java Client should default to setting mail.mime.allowutf8=true so that the " +
                "Jakarta Mail library allows UTF-8 characters in the header names of multipart response parts. " +
                "Normally, it only allows US-ASCII characters. But since MarkLogic allows UTF-8 characters in " +
                "URIs, we need the Jakarta Mail library (used by the Java Client) to be more permissive.");
    }

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

}
