/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import com.marklogic.client.row.RowSet;
import com.marklogic.client.type.PlanColumn;
import com.marklogic.spark.Options;
import com.marklogic.spark.ReadProgressLogger;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

/**
 * Reads triples from a batch of document URIs via the Optic fromTriples data accessor.
 */
class OpticTriplesReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(OpticTriplesReader.class);

    private static final String DATATYPE_COLUMN = "datatype";
    private static final String GRAPH_COLUMN = "graph";
    private static final String OBJECT_COLUMN = "object";

    private final UriBatcher uriBatcher;
    private final DatabaseClient databaseClient;
    private final DocumentContext documentContext;
    private final RowManager rowManager;
    private final PlanBuilder op;
    private final String graphBaseIri;

    // Only for logging
    private final long batchSize;
    private long progressCounter;

    private RowSet<RowRecord> currentRowSet;
    private Iterator<RowRecord> currentRowIterator;

    public OpticTriplesReader(ForestPartition forestPartition, DocumentContext context) {
        this.documentContext = context;
        this.graphBaseIri = context.getStringOption(Options.READ_TRIPLES_BASE_IRI);
        this.databaseClient = context.isDirectConnection() ?
            context.connectToMarkLogic(forestPartition.getHost()) :
            context.connectToMarkLogic();
        this.rowManager = this.databaseClient.newRowManager();
        this.op = this.rowManager.newPlanBuilder();

        final SearchQueryDefinition query = context.buildTriplesSearchQuery(this.databaseClient);
        final boolean filtered = context.getBooleanOption(Options.READ_TRIPLES_FILTERED, false);
        final boolean consistentSnapshot = context.isConsistentSnapshot();

        if (logger.isDebugEnabled()) {
            logger.debug("Will read from host {} for partition: {}; filtered: {}; consistent snapshot: {}",
                databaseClient.getHost(), forestPartition, filtered, consistentSnapshot);
        }

        this.uriBatcher = new UriBatcher(this.databaseClient, query, forestPartition, context.getBatchSize(), filtered, consistentSnapshot);
        this.batchSize = context.getBatchSize();
    }

    @Override
    public boolean next() throws IOException {
        if (currentRowIterator != null && currentRowIterator.hasNext()) {
            return true;
        }
        while (currentRowIterator == null || !currentRowIterator.hasNext()) {
            List<String> uris = uriBatcher.nextBatchOfUris();
            if (uris.isEmpty()) {
                return false; // End state; no more matching documents were found.
            }
            readNextBatchOfTriples(uris);
        }
        return true;
    }

    @Override
    public InternalRow get() {
        Object[] row = convertNextTripleIntoRow();
        progressCounter++;
        if (progressCounter >= batchSize) {
            ReadProgressLogger.logProgressIfNecessary(this.progressCounter);
            progressCounter = 0;
        }
        return new GenericInternalRow(row);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.currentRowSet);
    }

    private void readNextBatchOfTriples(@NotNull List<String> uris) {
        PlanBuilder.ModifyPlan plan = op
            .fromTriples(op.pattern(op.col("subject"), op.col("predicate"), op.col(OBJECT_COLUMN), op.graphCol(GRAPH_COLUMN)))
            .where(op.cts.documentQuery(op.xs.stringSeq(uris.toArray(new String[0]))));

        if (documentContext.hasOption(Options.READ_TRIPLES_GRAPHS)) {
            @NotNull String value = documentContext.getStringOption(Options.READ_TRIPLES_GRAPHS);
            String[] graphs = value.split(",");
            plan = plan.where(op.in(op.col(GRAPH_COLUMN), op.xs.stringSeq(graphs)));
        }

        plan = bindDatatypeAndLang(plan);
        this.currentRowSet = rowManager.resultRows(plan);
        this.currentRowIterator = this.currentRowSet.iterator();
    }

    /**
     * Ideally, fromTriples would allow for columns to be declared so that datatype and lang could be easily fetched.
     * Instead, we have to bind additional columns to retrieve these values.
     */
    private PlanBuilder.ModifyPlan bindDatatypeAndLang(PlanBuilder.ModifyPlan plan) {
        final PlanColumn objectCol = op.col(OBJECT_COLUMN);
        return plan.bindAs(DATATYPE_COLUMN, op.caseExpr(
            op.when(op.sem.isLiteral(objectCol), op.sem.datatype(objectCol)),
            op.elseExpr(op.sem.iri(op.xs.string("")))
        )).bindAs("lang", op.caseExpr(
            op.when(op.eq(op.col(DATATYPE_COLUMN), op.sem.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")), op.sem.lang(objectCol)),
            op.elseExpr(op.xs.string(""))
        ));
    }

    private Object[] convertNextTripleIntoRow() {
        RowRecord row = currentRowIterator.next();
        return new Object[]{
            getString(row, "subject"),
            getString(row, "predicate"),
            getString(row, OBJECT_COLUMN),
            getString(row, DATATYPE_COLUMN),
            getString(row, "lang"),
            getGraph(row)
        };
    }

    private UTF8String getGraph(RowRecord row) {
        String value = row.getString(GRAPH_COLUMN);
        if (this.graphBaseIri != null && isGraphRelative(value)) {
            value = this.graphBaseIri + value;
        }
        return value != null && !value.trim().isEmpty() ? UTF8String.fromString(value) : null;
    }

    private boolean isGraphRelative(String value) {
        try {
            return value != null && !(new URI(value).isAbsolute());
        } catch (URISyntaxException e) {
            // If the graph is not a valid URI, it is not an absolute URI, and thus the base IRI will be prepended.
            return true;
        }
    }

    private UTF8String getString(RowRecord row, String column) {
        String value = row.getString(column);
        return value != null && !value.trim().isEmpty() ? UTF8String.fromString(value) : null;
    }
}
