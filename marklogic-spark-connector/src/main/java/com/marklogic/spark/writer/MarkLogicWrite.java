/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.api.WriteListener;
import com.marklogic.spark.reader.customcode.CustomCodeContext;
import com.marklogic.spark.writer.customcode.CustomCodeWriterFactory;
import com.marklogic.spark.writer.rdf.GraphWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MarkLogicWrite implements BatchWrite, StreamingWrite {

    private final WriteContext writeContext;

    MarkLogicWrite(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public boolean useCommitCoordinator() {
        return BatchWrite.super.useCommitCoordinator();
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        int numPartitions = info.numPartitions();
        writeContext.setNumPartitions(numPartitions);
        DataWriterFactory dataWriterFactory = determineWriterFactory();
        if (dataWriterFactory instanceof WriteBatcherDataWriterFactory) {
            logPartitionAndThreadCounts(numPartitions);
        } else {
            Util.MAIN_LOGGER.info("Number of partitions: {}", numPartitions);
        }
        return dataWriterFactory;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0) {
            final WriteListener.CommitResults commitResults = aggregateCommitMessages(messages);
            if (!commitResults.getGraphs().isEmpty()) {
                try (DatabaseClient client = writeContext.connectToMarkLogic()) {
                    GraphWriter graphWriter = new GraphWriter(client, writeContext.getProperties().get(Options.WRITE_PERMISSIONS));
                    graphWriter.createGraphs(commitResults.getGraphs());
                }
            }

            invokeCommitListener(commitResults);

            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Success count: {}", commitResults.getSuccessCount());
                if (commitResults.getSkippedCount() > 0) {
                    Util.MAIN_LOGGER.info("Skipped count: {}", commitResults.getSkippedCount());
                }
            }
            if (commitResults.getFailureCount() > 0) {
                Util.MAIN_LOGGER.error("Failure count: {}", commitResults.getFailureCount());
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // No action. We may eventually want to show the partial progress via the commit messages.
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return (StreamingDataWriterFactory) determineWriterFactory();
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        commit(messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        abort(messages);
    }

    private DataWriterFactory determineWriterFactory() {
        if (Util.isWriteWithCustomCodeOperation(writeContext.getProperties())) {
            CustomCodeContext context = new CustomCodeContext(writeContext.getProperties(), writeContext.getSchema());
            return new CustomCodeWriterFactory(context);
        }

        // This is the last chance we have for accessing the hadoop config, which is needed by the writer.
        // SerializableConfiguration allows for it to be sent to the factory.
        SparkSession session = Util.getSparkSession();
        Configuration config = session.sparkContext().hadoopConfiguration();
        return new WriteBatcherDataWriterFactory(writeContext, new SerializableConfiguration(config));
    }

    private void logPartitionAndThreadCounts(int numPartitions) {
        int userDefinedPartitionThreadCount = writeContext.getUserDefinedThreadCountPerPartition();
        if (userDefinedPartitionThreadCount > 0) {
            Util.MAIN_LOGGER.info("Number of partitions: {}; total thread count: {}; thread count per partition: {}",
                numPartitions, numPartitions * userDefinedPartitionThreadCount, userDefinedPartitionThreadCount);
        } else {
            Util.MAIN_LOGGER.info("Number of partitions: {}; total threads used for writing: {}",
                numPartitions, writeContext.getTotalThreadCount());
        }
    }

    private WriteListener.CommitResults aggregateCommitMessages(WriterCommitMessage[] messages) {
        long successCount = 0;
        long failureCount = 0;
        long skippedCount = 0;
        Set<String> graphs = new HashSet<>();

        final int maxFailedDocumentCount = writeContext.getMaxFailedDocumentCount();
        final Map<String, String> failedDocuments = new HashMap<>();

        for (WriterCommitMessage message : messages) {
            CommitMessage msg = (CommitMessage) message;
            successCount += msg.successItemCount();
            failureCount += msg.failedItemCount();
            skippedCount += msg.skippedItemCount();
            if (msg.graphs() != null) {
                graphs.addAll(msg.graphs());
            }
            if (msg.failedDocuments() != null) {
                msg.failedDocuments().forEach((key, value) -> {
                    if (failedDocuments.size() < maxFailedDocumentCount) {
                        failedDocuments.put(key, value);
                    }
                });
            }
        }

        return new WriteListener.CommitResults(successCount, skippedCount, failureCount, graphs, failedDocuments);
    }

    private void invokeCommitListener(WriteListener.CommitResults commitResults) {
        final WriteListener writeListener = writeContext.makeWriteListener();
        if (writeListener != null) {
            Util.MAIN_LOGGER.debug("Invoking commit listener: {}", writeListener.getClass().getName());
            try {
                writeListener.onWriteCommit(commitResults);
            } catch (Exception e) {
                Util.MAIN_LOGGER.warn("Commit listener failed; cause: " + e.getMessage(), e);
            }

            if (writeListener instanceof Closeable) {
                try {
                    ((Closeable) writeListener).close();
                } catch (Exception e) {
                    Util.MAIN_LOGGER.warn("Failed to close write listener; cause: " + e.getMessage(), e);
                }
            }
        }
    }
}
