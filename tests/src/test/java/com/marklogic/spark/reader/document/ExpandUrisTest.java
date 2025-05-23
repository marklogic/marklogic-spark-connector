/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpandUrisTest extends AbstractIntegrationTest {

    @Test
    void javascript() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, """
                /author/author1.json
                /author/author2.json""")
            .option(Options.READ_EXPAND_URIS_JAVASCRIPT, """
                var URIs;
                const citationIds = cts.elementValues(xs.QName("CitationID"), null, null, cts.documentQuery(URIs));
                cts.uris(null, null, cts.andQuery([
                    cts.notQuery(cts.documentQuery(URIs)),
                    cts.collectionQuery('author'),
                    cts.jsonPropertyValueQuery('CitationID', citationIds)
                ]))
                """)
            .load()
            .select("uri")
            .collectAsList();

        assertEquals(5, rows.size(), "Expecting 5 URIs - the 2 original URIs and 3 expanded URIs. The 3 expanded " +
            "URIs come from the query on the CitationID property, where CitationID must be the value of CitationID " +
            "found in the original URIs.");

        List<String> uris = rows.stream().map(row -> row.getString(0)).toList();
        Stream.of("/author/author1.json", "/author/author2.json",
                "/author/author4.json", "/author/author8.json", "/author/author14.json")
            .forEach(uri -> assertTrue(uris.contains(uri), "Expecting to find URI: " + uri));
    }


}
