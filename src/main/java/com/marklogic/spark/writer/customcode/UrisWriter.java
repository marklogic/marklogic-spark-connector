/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.customcode.CustomCodeContext;
import com.marklogic.spark.writer.CommitMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class UrisWriter implements DataWriter<InternalRow> {

    private final List<String> currentBatch = new ArrayList<>();
    private final long batchSize;

    private final Consumer<List<String>> urisProcessor;
    private int successCount;

    public UrisWriter(CustomCodeContext customCodeContext) {
        this.batchSize = customCodeContext.getNumericOption(Options.WRITE_BATCH_SIZE, 100, 1);
        DatabaseClient databaseClient = customCodeContext.connectToMarkLogic();
        if (customCodeContext.hasOption(Options.WRITE_URIS_PATCH)) {
            urisProcessor = new OpticUrisProcessor(databaseClient, customCodeContext.getStringOption(Options.WRITE_URIS_PATCH));
        } else {
            urisProcessor = new EvalUrisProcessor(databaseClient)
                .withCollectionsToAdd(customCodeContext.getStringArrayOption(Options.WRITE_URIS_COLLECTIONS_ADD))
                .withCollectionsToRemove(customCodeContext.getStringArrayOption(Options.WRITE_URIS_COLLECTIONS_REMOVE))
                .withCollectionsToSet(customCodeContext.getStringArrayOption(Options.WRITE_URIS_COLLECTIONS_SET))
                .withPermissionsToAdd(customCodeContext.getStringArrayOption(Options.WRITE_URIS_PERMISSIONS_ADD))
                .withPermissionsToRemove(customCodeContext.getStringArrayOption(Options.WRITE_URIS_PERMISSIONS_REMOVE))
                .withPermissionsToSet(customCodeContext.getStringArrayOption(Options.WRITE_URIS_PERMISSIONS_SET));
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {
        currentBatch.add(record.getString(0));
        if (currentBatch.size() >= batchSize) {
            processCurrentBatch();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (!currentBatch.isEmpty()) {
            processCurrentBatch();
        }
        return new CommitMessage(successCount, 0, null);
    }

    @Override
    public void abort() {
        // No action
    }

    @Override
    public void close() throws IOException {
        // Close the client?
    }

    private void processCurrentBatch() {
        urisProcessor.accept(currentBatch);
        successCount += currentBatch.size();
        currentBatch.clear();
    }
}
