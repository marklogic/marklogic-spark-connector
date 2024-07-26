package com.marklogic.spark.writer.rdf;

import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

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

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
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
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("src/test/resources/rdf/mini-taxonomy.xml")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

}
