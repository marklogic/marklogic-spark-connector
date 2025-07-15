/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.triples;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadTriplesTest extends AbstractIntegrationTest {

    @Test
    void graph() {
        List<Row> rows = startRead()
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .load().collectAsList();

        assertEquals(8, rows.size());

        // Verify a row with both datatype and lang.
        Row langRow = rows.stream().filter(row -> "Debt Management".equals(row.getString(2))).findFirst().get();
        assertEquals("http://vocabulary.worldbank.org/taxonomy/451", langRow.getString(0));
        assertEquals("http://www.w3.org/2004/02/skos/core#prefLabel", langRow.getString(1));
        assertEquals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", langRow.getString(3));
        assertEquals("en", langRow.getString(4));
        assertEquals("http://example.org/graph", langRow.getString(5));

        // Verify a row with a datatype but no lang.
        Row creatorRow = rows.stream().filter(row -> "http://purl.org/dc/terms/creator".equals(row.getString(1))).findFirst().get();
        assertEquals("wb", creatorRow.getString(2));
        assertEquals("http://www.w3.org/2001/XMLSchema#string", creatorRow.getString(3));
        assertTrue(creatorRow.isNullAt(4));

        // Verify a row with neither a datatype nor a lang.
        Row conceptRow = rows.stream().filter(row -> "http://www.w3.org/2004/02/skos/core#Concept".equals(row.getString(2))).findFirst().get();
        assertTrue(conceptRow.isNullAt(3));
        assertTrue(conceptRow.isNullAt(4));
    }

    @Test
    void defaultGraph() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/mini-taxonomy.xml")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_PREFIX, "/defaultgraph")
            .mode(SaveMode.Append)
            .save();

        List<Row> rows = startRead()
            .option(Options.READ_TRIPLES_DIRECTORY, "/defaultgraph/")
            .load().collectAsList();

        assertEquals(8, rows.size());
        rows.forEach(row -> assertEquals("http://marklogic.com/semantics#default-graph",
            row.getString(5), "Since no graph was specified when the triples were loaded, the triples should have " +
                "been assigned to MarkLogic's default graph. That value should then be in the 'graph' column for " +
                "each of the triples when they're read back."));
    }

    @Test
    void multipleGraphs() {
        long count = startRead()
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph,other-graph")
            .load().count();

        assertEquals(16, count, "Should include the 8 triples from each test file in the 2 given graphs.");
    }

    @Test
    void collections() {
        long count = startRead()
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://example.org/graph")
            .load().count();

        assertEquals(16, count, "We expect 16 triples back as MarkLogic sees each triple on the test document " +
            "as belonging to two graphs - 'http://example.org/graph' and 'test-config'.");
    }

    @Test
    void twoCollections() {
        long count = startRead()
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://example.org/graph,other-graph")
            .option(Options.READ_BATCH_SIZE, 5)
            .option(Options.READ_LOG_PROGRESS, 10)
            .load().count();

        assertEquals(32, count, "Since both test triples files belong to 'test-config', and each also belongs to " +
            "a second collection, the 8 triples in each file are returned twice - once for each collection - " +
            "producing a total of 32 triples.");
    }

    @Test
    void graphAndCollection() {
        long count = startRead()
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .option(Options.READ_TRIPLES_COLLECTIONS, "test-config")
            .load().count();

        assertEquals(8, count, "When both graphs and collections are specified, they should result in a collection " +
            "query that constrains on both collections. But since a graph is specified, we only expect back the 8 " +
            "triples in the given graph.");
    }

    @Test
    void directory() {
        long count = startRead()
            .option(Options.READ_TRIPLES_DIRECTORY, "/triples/")
            .load().count();

        assertEquals(16, count, "Since no graph is specified, we should get 16 triples, as each of the 8 triples " +
            "is associated with 2 collections. The directory option ensures we pull from only one of the two test " +
            "triples files.");
    }

    @Test
    void stringQueryWithOptions() {
        long count = startRead()
            .option(Options.READ_TRIPLES_STRING_QUERY, "tripleObject:\"http://vocabulary.worldbank.org/taxonomy/1107\"")
            .option(Options.READ_TRIPLES_OPTIONS, "test-options")
            .load().count();

        assertEquals(16, count, "Since no graph is specified, we should get 16 triples, as each of the 8 triples " +
            "is associated with 2 collections. The string query plus the options ensures we only get triples from =" +
            "the test file that has a sem:object value of 'http://vocabulary.worldbank.org/taxonomy/1107'.");
    }

    @Test
    void structuredQuery() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Debt Management</text></term-query></query>";

        long count = startRead()
            .option(Options.READ_TRIPLES_QUERY, query)
            .load().count();

        assertEquals(16, count, "Since no graph is specified, we should get 16 triples, as each of the 8 triples " +
            "is associated with 2 collections. And we should only get triples from the test triples document that " +
            "has the term 'Debt Management in it.");
    }

    @Test
    void uris() {
        long count = startRead()
            .option(Options.READ_TRIPLES_URIS, "/triples/mini-taxonomy.xml\n/other-triples/other-taxonomy.xml")
            .load().count();

        assertEquals(32, count, "Since no graph is specified, the 8 triples in each test triples document get " +
            "returned twice, once per collection assigned to the document.");
    }

    private DataFrameReader startRead() {
        return newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }
}
