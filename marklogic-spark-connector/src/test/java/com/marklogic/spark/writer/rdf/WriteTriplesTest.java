/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.rdf;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteTriplesTest extends AbstractWriteRdfTest {

    @Test
    void triplesInDefaultGraph() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertTripleCount(RdfRowConverter.DEFAULT_MARKLOGIC_GRAPH, 8,
            "Triples should be added to the default MarkLogic graph when the user does not specify a graph.");
    }

    @Test
    void datatypeUsage() {
        readRdfAndWrite("src/test/resources/rdf/datatype-test.ttl")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH, "datatype-graph")
            .mode(SaveMode.Append)
            .save();

        List<String> uris = getUrisInCollection("datatype-graph", 2);
        String triplesUri = uris.stream().filter(uri -> uri.startsWith("/triplestore")).findFirst().get();
        XmlNode doc = readXmlDocument(triplesUri);

        doc.assertElementExists(
            "The Jane object doesn't have a language, so it should have a datatype and no xml:lang.",
            "/sem:triples/sem:triple/sem:object[@datatype = 'http://www.w3.org/2001/XMLSchema#string' and . = 'Jane' and not(@xml:lang)]"
        );
        doc.assertElementExists(
            "The Smith object does have a language, so it should have an xml:lang and no datatype. This avoids a bug " +
                "in MarkLogic 11 where having both datatype and xml:lang results in an empty lang value. That bug " +
                "does not occur in MarkLogic 12.",
            "/sem:triples/sem:triple/sem:object[not(@datatype) and . = 'Smith' and @xml:lang='en-US']"
        );
        doc.assertElementExists(
            "For a non-string value, a datatype should exist with no xml:lang.",
            "/sem:triples/sem:triple/sem:object[@datatype = 'http://www.w3.org/2001/XMLSchema#integer' and . = '1111111111' and not(@xml:lang)]"
        );

        GraphManager graphManager = getDatabaseClient().newGraphManager();
        String graphString = graphManager.things(new StringHandle().withFormat(Format.XML), "http://marklogicsparql.com/id#1111").get();
        XmlNode graph = new XmlNode(graphString,
            Namespace.getNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            Namespace.getNamespace("ab", "http://marklogicsparql.com/addressbook#"));

        // Verify that the "things" representation of the triples as XML matches the same conditions expressed above
        // on the raw triples document.
        graph.assertElementExists("//ab:firstName[@rdf:datatype='http://www.w3.org/2001/XMLSchema#string' and . = 'Jane' and not(@xml:lang)]");
        graph.assertElementExists("//ab:lastName[@xml:lang='en-us' and . = 'Smith' and not(@datatype)]");
        graph.assertElementExists("//ab:homeTel[@rdf:datatype='http://www.w3.org/2001/XMLSchema#integer' and . = '1111111111' and not(@xml:lang)]");
    }

    @Test
    void triplesInUserDefinedGraph() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH, "my-graph")
            .mode(SaveMode.Append)
            .save();

        assertTripleCount("my-graph", 8,
            "The spark.marklogic.write.graph option should be used to specify a graph for each triple.");
    }

    @Test
    void triplesInGraphOverride() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH_OVERRIDE, "my-graph-override")
            .mode(SaveMode.Append)
            .save();

        assertTripleCount("my-graph-override", 8,
            "The spark.marklogic.write.graphOverride option should function the exact same way as " +
                "spark.marklogic.write.graph for triples, as both are specifying a graph for every triple.");
    }

    @Test
    void bothGraphAndGraphOverrideSpecified() {
        DataFrameWriter writer = readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_GRAPH, "my-graph")
            .option(Options.WRITE_GRAPH_OVERRIDE, "my-graph-override")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(writer::save);
        assertEquals("Can only specify one of spark.marklogic.write.graph and spark.marklogic.write.graphOverride.",
            ex.getMessage());
    }

    @Test
    void writeTwiceWithDifferentPermissions() {
        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        readRdfAndWrite()
            .option(Options.WRITE_PERMISSIONS, "rest-extension-user,read,rest-extension-user,update")
            .option(Options.WRITE_COLLECTIONS, "second-write")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester tester = readDocumentPermissions(getUrisInCollection("second-write", 1).get(0));
        tester.assertReadPermissionExists("rest-extension-user");
        tester.assertUpdatePermissionExists("rest-extension-user");
        assertEquals(1, tester.getDocumentPermissions().size(), "Should only have permissions " +
            "for the rest-extension-user role. Not sure if this is correct though. MLCP does some upfront work " +
            "to retrieve permissions for any existing graph and then reuses those, ignoring what the user " +
            "specifies. That behavior is not documented though and may seem surprising to a user.");
    }

    private DataFrameWriter<Row> readRdfAndWrite() {
        return readRdfAndWrite("src/test/resources/rdf/mini-taxonomy.xml");
    }

    private DataFrameWriter<Row> readRdfAndWrite(String path) {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load(path)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

}
