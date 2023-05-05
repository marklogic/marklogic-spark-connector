package com.marklogic.spark.writer;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class WriteContext extends ContextSupport {

    final static long serialVersionUID = 1;

    private final StructType schema;

    public WriteContext(StructType schema, Map<String, String> properties) {
        super(properties);
        this.schema = schema;
    }

    public StructType getSchema() {
        return schema;
    }

    public WriteBatcher newWriteBatcher(DataMovementManager dataMovementManager) {
        WriteBatcher writeBatcher = dataMovementManager
            .newWriteBatcher()
            .withBatchSize((int) getNumericOption(Options.WRITE_BATCH_SIZE, 100, 1))
            .withThreadCount((int) getNumericOption(Options.WRITE_THREAD_COUNT, 4, 1));

        if (logger.isDebugEnabled()) {
            writeBatcher.onBatchSuccess(this::logBatchOnSuccess);
        }

        // TODO Make this all configurable
        writeBatcher.withDefaultMetadata(new DocumentMetadataHandle()
            .withCollections("my-test-data")
            .withPermission("spark-user-role", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE));

        return writeBatcher;
    }

    private void logBatchOnSuccess(WriteBatch batch) {
        int docCount = batch.getItems().length;
        if (docCount > 0) {
            WriteEvent firstEvent = batch.getItems()[0];
            // If the first event is the item added by DMSDK for the default metadata object, ignore it when showing
            // the count of documents in the batch.
            // the count of documents in the batch.
            if (firstEvent.getTargetUri() == null && firstEvent.getMetadata() != null) {
                docCount--;
            }
        }
        logger.debug("Wrote batch; length: {}; job batch number: {}", docCount, batch.getJobBatchNumber());
    }
}
