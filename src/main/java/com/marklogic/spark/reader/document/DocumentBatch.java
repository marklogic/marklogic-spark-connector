package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.SearchQueryDefinition;
import com.marklogic.spark.Options;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentBatch implements Batch {

    private static final Logger logger = LoggerFactory.getLogger(DocumentBatch.class);

    private DocumentContext context;

    DocumentBatch(CaseInsensitiveStringMap options) {
        this.context = new DocumentContext(options);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        DatabaseClient client = this.context.connectToMarkLogic();
        if (this.context.isPageRangeStrategy()) {
            SearchQueryDefinition query = this.context.buildSearchQuery(client);
            SearchHandle searchResults = client.newQueryManager().search(query, new SearchHandle());
            final long serverTimestamp = searchResults.getServerTimestamp();
            final long total = searchResults.getTotalResults();
            final int batchSize = context.getBatchSize();
            // The max number of partitions we'd ever want, even if the user asks for more.
            final int maxPartitions = (int) Math.ceil((double) total / (double) batchSize);
            int partitionCount = 8;
            if (batchSize >= total) {
                partitionCount = 1;
            } else if (context.hasOption(Options.READ_NUM_PARTITIONS)) {
                partitionCount = Integer.parseInt(context.getProperties().get(Options.READ_NUM_PARTITIONS));
            }
            if (partitionCount > maxPartitions) {
                partitionCount = maxPartitions;
            }

            logger.info("Total results: {}; partition count: {}", total, partitionCount);

            final int pageRangeLength = (int) Math.ceil((double) total / (double) partitionCount);
            InputPartition[] partitions = new InputPartition[partitionCount];
            int counter = 0;
            // Interestingly, the batch size doesn't matter here - we just need to setup the initial ranges based
            // on the number of partitions. Batch size only matters for each call that a partition reader makes.
            for (long i = 1; i <= total; i += pageRangeLength) {
                long end = i + pageRangeLength > total ? total : i + pageRangeLength - 1;
                partitions[counter] = new PageRangePartition(i, end, serverTimestamp);
                counter++;
            }
            return partitions;
        } else {
            Forest[] forests = client.newDataMovementManager().readForestConfig().listForests();
            InputPartition[] partitions = new InputPartition[forests.length];
            for (int i = 0; i < forests.length; i++) {
                partitions[i] = new ForestPartition(forests[i].getForestName());
            }
            return partitions;
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return this.context.isPageRangeStrategy() ?
            new PageRangeReaderFactory(this.context) :
            new ForestReaderFactory(this.context);
    }
}
