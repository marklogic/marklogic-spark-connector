/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.rdf;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.writer.RowConverter;
import com.marklogic.spark.writer.WriteContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

/**
 * Converts each row into a sem:triple element, which is then added to a sem:triples XML document associated with a
 * graph.
 */
public class RdfRowConverter implements RowConverter {

    static final String DEFAULT_MARKLOGIC_GRAPH = "http://marklogic.com/semantics#default-graph";
    static final String DEFAULT_JENA_GRAPH = "urn:x-arq:DefaultGraphNode";

    private static final Logger logger = LoggerFactory.getLogger(RdfRowConverter.class);

    // Need to keep track of each graph that is seen in the rows so that they can eventually be created in MarkLogic
    // if they don't yet exist.
    private final Set<String> graphs = new HashSet<>();

    // Map of graph name to documents containing sem:triple elements.
    private Map<String, TriplesDocument> triplesDocuments = new HashMap<>();

    private final String defaultGraph;
    private final String graphOverride;

    public RdfRowConverter(WriteContext writeContext) {
        String graph = writeContext.getStringOption(Options.WRITE_GRAPH);
        String tempGraphOverride = writeContext.getStringOption(Options.WRITE_GRAPH_OVERRIDE);
        if (graph != null && tempGraphOverride != null) {
            throw new ConnectorException(String.format("Can only specify one of %s and %s.",
                writeContext.getOptionNameForMessage(Options.WRITE_GRAPH),
                writeContext.getOptionNameForMessage(Options.WRITE_GRAPH_OVERRIDE)));
        }
        if (graph != null) {
            this.defaultGraph = graph;
            this.graphOverride = null;
        } else if (tempGraphOverride != null) {
            this.defaultGraph = tempGraphOverride;
            this.graphOverride = tempGraphOverride;
        } else {
            this.defaultGraph = DEFAULT_MARKLOGIC_GRAPH;
            this.graphOverride = null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Default graph: {}", defaultGraph);
        }
    }

    @Override
    public Iterator<DocumentInputs> convertRow(InternalRow row) {
        final String graph = determineGraph(row);
        graphs.add(graph);

        TriplesDocument triplesDocument;
        if (triplesDocuments.containsKey(graph)) {
            triplesDocument = triplesDocuments.get(graph);
        } else {
            triplesDocument = new TriplesDocument(graph);
            triplesDocuments.put(graph, triplesDocument);
        }

        triplesDocument.addTriple(row);
        if (triplesDocument.hasMaxTriples()) {
            triplesDocuments.remove(graph);
            return Stream.of(triplesDocument.buildDocument()).iterator();
        }
        return Stream.<DocumentInputs>empty().iterator();
    }

    /**
     * Return a DocumentInputs for each triples document that has not yet reached "max triples".
     *
     * @return
     */
    @Override
    public Iterator<DocumentInputs> getRemainingDocumentInputs() {
        return this.triplesDocuments.values().stream()
            .map(TriplesDocument::buildDocument)
            .iterator();
    }

    /**
     * Allows WriteBatcherDataWriter to access all the graphs that this class has seen.
     *
     * @return
     */
    public Set<String> getGraphs() {
        return graphs;
    }

    private String determineGraph(InternalRow row) {
        if (graphOverride != null) {
            return graphOverride;
        }
        if (row.isNullAt(5)) {
            return defaultGraph;
        }
        String graph = row.getString(5);
        return DEFAULT_JENA_GRAPH.equals(graph) ? defaultGraph : graph;
    }

}
