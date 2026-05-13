/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.ConnectorException;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class MarkLogicWrite implements BatchWrite, StreamingWrite {

    private final WriteContext writeContext;
    private final Consumer<Map<String, Object>> commitResultsConsumer;

    MarkLogicWrite(WriteContext writeContext) {
        this.writeContext = writeContext;
        this.commitResultsConsumer = instantiateCommitResultsConsumer();
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
                try (DatabaseClient client = writeContext.connectToMarkLogic()) {
                    GraphWriter graphWriter = new GraphWriter(client, writeContext.getProperties().get(Options.WRITE_PERMISSIONS));
                    graphWriter.createGraphs(commitResults.graphs);
                }
            }

            if (commitResultsConsumer != null) {
                Map<String, Object> resultsMap = new HashMap<>();
                resultsMap.put("successCount", commitResults.successCount);
                resultsMap.put("failureCount", commitResults.failureCount);
                resultsMap.put("skippedCount", commitResults.skippedCount);
                commitResultsConsumer.accept(resultsMap);
            }

            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Success count: {}", commitResults.successCount);
                if (commitResults.skippedCount > 0) {
                    Util.MAIN_LOGGER.info("Skipped count: {}", commitResults.skippedCount);
                }
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

    private CommitResults aggregateCommitMessages(WriterCommitMessage[] messages) {
        int successCount = 0;
        int failureCount = 0;
        int skippedCount = 0;
        Set<String> graphs = new HashSet<>();
        for (WriterCommitMessage message : messages) {
            CommitMessage msg = (CommitMessage) message;
            successCount += msg.successItemCount();
            failureCount += msg.failedItemCount();
            skippedCount += msg.skippedItemCount();
            if (msg.graphs() != null) {
                graphs.addAll(msg.graphs());
            }
        }
        return new CommitResults(successCount, failureCount, skippedCount, graphs);
    }

    /**
     * Aggregates the results of each CommitMessage.
     */
    private record CommitResults(int successCount, int failureCount, int skippedCount, Set<String> graphs) {
    }

    @SuppressWarnings("unchecked")
    private Consumer<Map<String, Object>> instantiateCommitResultsConsumer() {
        String className = writeContext.getProperties().get(Options.WRITE_COMMIT_RESULTS_CONSUMER_CLASSNAME);
        if (className != null && !className.trim().isEmpty()) {
            try {
                Class<?> clazz = Class.forName(className);
                Object instance = clazz.getDeclaredConstructor().newInstance();
                if (instance instanceof Consumer) {
                    return (Consumer<Map<String, Object>>) instance;
                } else {
                    throw new ConnectorException(String.format(
                        "Class %s does not implement Consumer interface", className));
                }
            } catch (ConnectorException ce) {
                throw ce;
            } catch (Exception e) {
                throw new ConnectorException(String.format(
                    "Unable to instantiate commit results consumer: %s; cause: %s", className, e.getMessage()), e);
            }
        }
        return null;
    }

}
