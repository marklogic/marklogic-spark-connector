/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.rdf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.ext.helper.ClientHelper;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests "triple rows" where the "graph" column is populated, hence making it a quad.
 */
class WriteQuadsTest extends AbstractWriteRdfTest {

    private static final String GRAPH_COLLECTION = "http://marklogic.com/semantics#graphs";

    @Test
    void quads() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS + ",qconsole-user,read")
            .option(Options.WRITE_COLLECTIONS, "test-triples")
            .mode(SaveMode.Append)
            .save();

        verifyFourGraphsExist();
        verifyGraphPermissions();
        assertCollectionSize("Expecting 1 triples document for each graph, as there's only 1 partition writer.",
            "test-triples", 4);

        assertTripleCount("http://www.example.org/exampleDocument#G1", 4);
        assertTripleCount("http://www.example.org/exampleDocument#G2", 2);
        assertTripleCount("http://www.example.org/exampleDocument#G3", 9);
        assertTripleCount(RdfRowConverter.DEFAULT_MARKLOGIC_GRAPH, 1);
    }

    @Test
    void quadsInUserDefinedGraph() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH, "my-graph")
            .mode(SaveMode.Append)
            .save();

        assertTripleCount("my-graph", 1, "The one triple in the quads file without a graph is first " +
            "assigned by Jena to the urn:x-arq:DefaultGraphNode graph. The user-defined graph is then used instead of that.");
        assertTripleCount("http://www.example.org/exampleDocument#G1", 4);
        assertTripleCount("http://www.example.org/exampleDocument#G2", 2);
        assertTripleCount("http://www.example.org/exampleDocument#G3", 9);
    }

    @Test
    void quadsWithGraphOverride() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH_OVERRIDE, "my-graph-override")
            .mode(SaveMode.Append)
            .save();

        assertTripleCount("my-graph-override", 16, "The 'graph override' value should override the graph in every " +
            "quad, regardless of whether it has a graph value or not.");

        assertCollectionSize(GRAPH_COLLECTION, 1);
    }

    @Test
    void threeQuadsTwoPartitions() {
        readRdf("src/test/resources/rdf/three-quads.trig")
            .repartition(2)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "multiple-partitions")
            .mode(SaveMode.Append)
            .save();

        verifyFourGraphsExist();
        assertTripleCount("http://www.example.org/exampleDocument#G1", 4);
        assertTripleCount("http://www.example.org/exampleDocument#G2", 2);
        assertTripleCount("http://www.example.org/exampleDocument#G3", 9);
        assertTripleCount(RdfRowConverter.DEFAULT_MARKLOGIC_GRAPH, 1);

        List<String> uris = new ClientHelper(getDatabaseClient()).getUrisInCollection("multiple-partitions");
        assertTrue(uris.size() > 4, "With 2 partition writers, we expect more than 4 triple documents because it is " +
            "almost certainly the case that the two writers get triples from the same graph. Since they're independent " +
            "of each other, they'll both create a separate triples document in the same graph. This means a user can " +
            "end up with far fewer than 100 triples in one document, but that doesn't really have any functional " +
            "impact. The user primarily cares that the triples are in the triple index and queryable via the correct " +
            "graph. Actual count of triple document URIs: " + uris.size());
    }

    private Dataset<Row> readRdf(String path) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load(path);
    }

    private DataFrameWriter<Row> readRdfAndWrite() {
        return readRdf("src/test/resources/rdf/three-quads.trig")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

    private void verifyFourGraphsExist() {
        assertCollectionSize(GRAPH_COLLECTION, 4);
        assertInCollections("http://www.example.org/exampleDocument#G1", GRAPH_COLLECTION);
        assertInCollections("http://www.example.org/exampleDocument#G2", GRAPH_COLLECTION);
        assertInCollections("http://www.example.org/exampleDocument#G3", GRAPH_COLLECTION);

        // The MarkLogic default graph should be used for the one triple that is assigned to the Jena default
        // graph - urn:x-arq:DefaultGraphNode.
        assertInCollections(RdfRowConverter.DEFAULT_MARKLOGIC_GRAPH, GRAPH_COLLECTION);
    }

    /**
     * This verifies the permissions on each graph document, which are assumed to be set via the
     * sem:create-graph-document function. It also does a check on the output of sem.graphGetPermissions, verifying
     * that the 3 expected permissions are present.
     */
    private void verifyGraphPermissions() {
        XMLDocumentManager mgr = getDatabaseClient().newXMLDocumentManager();
        String[] graphUris = new String[]{
            "http://www.example.org/exampleDocument#G1",
            "http://www.example.org/exampleDocument#G2",
            "http://www.example.org/exampleDocument#G3",
            "http://www.example.org/exampleDocument#G3"
        };

        for (String graphUri : graphUris) {
            ArrayNode perms = (ArrayNode) getDatabaseClient().newServerEval()
                .javascript(String.format("sem.graphGetPermissions('%s')", graphUri))
                .evalAs(JsonNode.class);
            assertEquals(3, perms.size(), "Expecting 2 permissions for spark-user-role and 1 for qconsole-user.");
        }

        DocumentPage page = mgr.readMetadata(graphUris);
        while (page.hasNext()) {
            DocumentMetadataHandle metadata = page.next().getMetadata(new DocumentMetadataHandle());
            DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
            assertEquals(2, perms.size(), "Expecting qconsole-user and spark-user-role as the 2 roles containing permissions.");
            PermissionsTester tester = new PermissionsTester(metadata.getPermissions());
            tester.assertUpdatePermissionExists("spark-user-role");
            tester.assertReadPermissionExists("spark-user-role");
            tester.assertReadPermissionExists("qconsole-user");
        }
    }
}
