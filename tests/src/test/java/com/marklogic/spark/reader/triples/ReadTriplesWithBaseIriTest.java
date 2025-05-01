/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.triples;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadTriplesWithBaseIriTest extends AbstractIntegrationTest {

    @Test
    void relativeGraph() {
        List<Row> rows = startRead()
            .option(Options.READ_TRIPLES_GRAPHS, "other-graph")
            .option(Options.READ_TRIPLES_BASE_IRI, "/my-base-iri/")
            .load().collectAsList();

        assertEquals(8, rows.size());
        rows.forEach(row -> assertEquals("/my-base-iri/other-graph", row.getString(5), "When the triple's graph " +
            "value is relative, the user-provided base IRI should be prepended to it."));
    }

    @Test
    void absoluteGraph() {
        List<Row> rows = startRead()
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .option(Options.READ_TRIPLES_BASE_IRI, "/my-base-iri")
            .load().collectAsList();

        assertEquals(8, rows.size());
        rows.forEach(row -> assertEquals("http://example.org/graph", row.getString(5), "When the triple's graph " +
            "is absolute, the user-provided base IRI should not be prepended to it."));
    }

    private DataFrameReader startRead() {
        return newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }
}
