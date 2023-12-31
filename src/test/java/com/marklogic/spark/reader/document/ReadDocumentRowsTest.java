package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadDocumentRowsTest extends AbstractIntegrationTest {

    @Test
    void readByCollection() throws Exception {
        Dataset<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
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
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "some-collection-with-no-documents")
            .load()
            .count();
        assertEquals(0, count);
    }

}
