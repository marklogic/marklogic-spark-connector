package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.SearchQueryDefinition;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentBatch implements Batch {

    private static final Logger logger = LoggerFactory.getLogger(DocumentBatch.class);

    private final DocumentContext context;

    DocumentBatch(DocumentContext context) {
        this.context = context;
    }

    /**
     * Reuses the DMSDK support for obtaining a list of all eligible forests. A partition reader will then be
     * created for each partition/forest.
     *
     * @return
     */
    @Override
    public InputPartition[] planInputPartitions() {
        DatabaseClient client = this.context.connectToMarkLogic();
        Forest[] forests = client.newDataMovementManager().readForestConfig().listForests();

        SearchQueryDefinition query = this.context.buildSearchQuery(client);
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
        ForestPartition[] partitions = planner.makePartitions(estimate, serverTimestamp, forests);
        if (logger.isDebugEnabled()) {
            logger.debug("Created {} forest partitions", partitions.length);
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ForestReaderFactory(this.context);
    }
}
