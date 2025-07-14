/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class MarkLogicWrite implements BatchWrite, StreamingWrite {

    private WriteContext writeContext;

    // Used solely for testing. Will never be populated in a real world scenario.
    private static Consumer<Integer> successCountConsumer;
    private static Consumer<Integer> failureCountConsumer;

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
            final CommitResults commitResults = aggregateCommitMessages(messages);
            if (!commitResults.graphs.isEmpty()) {
                new GraphWriter(
                    writeContext.connectToMarkLogic(),
                    writeContext.getProperties().get(Options.WRITE_PERMISSIONS)
                ).createGraphs(commitResults.graphs);
            }

            if (successCountConsumer != null) {
                successCountConsumer.accept(commitResults.successCount);
            }
            if (failureCountConsumer != null) {
                failureCountConsumer.accept(commitResults.failureCount);
            }

            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Success count: {}", commitResults.successCount);
            }
            if (commitResults.failureCount > 0) {
                Util.MAIN_LOGGER.error("Failure count: {}", commitResults.failureCount);
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
        Configuration config = SparkSession.active().sparkContext().hadoopConfiguration();
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

    private CommitResults aggregateCommitMessages(WriterCommitMessage[] messages) {
        int successCount = 0;
        int failureCount = 0;
        Set<String> graphs = new HashSet<>();
        for (WriterCommitMessage message : messages) {
            CommitMessage msg = (CommitMessage) message;
            successCount += msg.getSuccessItemCount();
            failureCount += msg.getFailedItemCount();
            if (msg.getGraphs() != null) {
                graphs.addAll(msg.getGraphs());
            }
        }
        return new CommitResults(successCount, failureCount, graphs);
    }

    /**
     * Aggregates the results of each CommitMessage.
     */
    private static class CommitResults {
        final int successCount;
        final int failureCount;
        final Set<String> graphs;

        public CommitResults(int successCount, int failureCount, Set<String> graphs) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.graphs = graphs;
        }
    }

    public static void setSuccessCountConsumer(Consumer<Integer> consumer) {
        successCountConsumer = consumer;
    }

    public static void setFailureCountConsumer(Consumer<Integer> consumer) {
        failureCountConsumer = consumer;
    }

}
