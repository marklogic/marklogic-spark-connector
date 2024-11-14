/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.file.TripleRowSchema;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentBatch implements Batch {

    private static final Logger logger = LoggerFactory.getLogger(DocumentBatch.class);

    private final DocumentContext context;
    private final InputPartition[] partitions;

    /**
     * Reuses the DMSDK support for obtaining a list of all eligible forests. A partition reader will then be
     * created for each partition/forest.
     */
    DocumentBatch(DocumentContext context) {
        this.context = context;

        DatabaseClient client = this.context.connectToMarkLogic();
        Forest[] forests = client.newDataMovementManager().readForestConfig().listForests();

        SearchQueryDefinition query = TripleRowSchema.SCHEMA.equals(context.getSchema()) ?
            this.context.buildTriplesSearchQuery(client) :
            this.context.buildSearchQuery(client);
        
        // Must null this out so SearchHandle still works below.
        query.setResponseTransform(null);

        QueryManager queryManager = client.newQueryManager();
        queryManager.setPageLength(1);

        SearchHandle handle = queryManager.search(query, new SearchHandle());
        final long estimate = handle.getTotalResults();
        final long serverTimestamp = handle.getServerTimestamp();

        if (logger.isDebugEnabled()) {
            logger.debug("Creating forest partitions; query estimate: {}; server timestamp: {}", estimate, serverTimestamp);
        }
        ForestPartitionPlanner planner = new ForestPartitionPlanner(context.getPartitionsPerForest());
        this.partitions = planner.makePartitions(estimate, serverTimestamp, forests);

        if (Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("Created {} partitions; query estimate: {}", partitions.length, estimate);
        }
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return this.partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ForestReaderFactory(this.context);
    }
}
