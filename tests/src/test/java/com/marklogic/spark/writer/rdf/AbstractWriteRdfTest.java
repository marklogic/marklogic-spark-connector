/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.rdf;

import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractWriteRdfTest extends AbstractIntegrationTest {

    private GraphManager graphManager;

    @BeforeEach
    void beforeEach() {
        graphManager = getDatabaseClient().newGraphManager();
    }

    protected final XmlNode readTriplesInGraph(String graph) {
        String content = graphManager.read(graph, new StringHandle().withMimetype(RDFMimeTypes.TRIPLEXML)).get();
        return new XmlNode(content, TriplesDocument.SEMANTICS_NAMESPACE);
    }

    protected final void assertTripleCount(String graph, int count, String message) {
        XmlNode doc = readTriplesInGraph(graph);
        doc.assertElementCount(message, "/sem:triples/sem:triple", count);
    }
    
    protected final void assertTripleCount(String graph, int count) {
        XmlNode doc = readTriplesInGraph(graph);
        doc.assertElementCount("/sem:triples/sem:triple", count);
    }
}
