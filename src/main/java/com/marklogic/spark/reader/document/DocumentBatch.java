package com.marklogic.spark.reader.document;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DocumentBatch implements Batch {

    private DocumentContext context;

    DocumentBatch(CaseInsensitiveStringMap options) {
        this.context = new DocumentContext(options);
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
        InputPartition[] partitions = new InputPartition[forests.length];
        for (int i = 0; i < forests.length; i++) {
            partitions[i] = new ForestPartition(forests[i].getForestName());
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ForestReaderFactory(this.context);
    }
}
