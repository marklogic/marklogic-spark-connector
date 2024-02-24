/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.writer;

import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.customcode.CustomCodeContext;
import com.marklogic.spark.writer.customcode.CustomCodeWriterFactory;
import com.marklogic.spark.writer.rdf.GraphWriter;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class MarkLogicWrite implements BatchWrite, StreamingWrite {

    private static final Logger logger = LoggerFactory.getLogger("com.marklogic.spark");

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
        int partitions = info.numPartitions();
        int threadCount = writeContext.getThreadCount();
        Util.MAIN_LOGGER.info("Number of partitions: {}; thread count per partition: {}; total threads used for writing: {}",
            partitions, threadCount, partitions * threadCount);
        return (DataWriterFactory) determineWriterFactory();
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
        if (messages != null && messages.length > 0 && messages[0] != null) {
            Util.MAIN_LOGGER.warn("Abort messages received: {}", Arrays.asList(messages));
        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return (StreamingDataWriterFactory) determineWriterFactory();
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0 && logger.isDebugEnabled()) {
            logger.debug("Commit messages received for epochId {}: {}", epochId, Arrays.asList(messages));
        }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0) {
            Util.MAIN_LOGGER.warn("Abort messages received for epochId {}: {}", epochId, Arrays.asList(messages));
        }
    }

    private Object determineWriterFactory() {
        if (writeContext.hasOption(Options.WRITE_INVOKE, Options.WRITE_JAVASCRIPT, Options.WRITE_XQUERY)) {
            CustomCodeContext context = new CustomCodeContext(
                writeContext.getProperties(), writeContext.getSchema(), Options.WRITE_VARS_PREFIX
            );
            return new CustomCodeWriterFactory(context);
        }
        return new WriteBatcherDataWriterFactory(writeContext);
    }

    private CommitResults aggregateCommitMessages(WriterCommitMessage[] messages) {
        if (logger.isDebugEnabled()) {
            logger.debug("Commit messages received: {}", Arrays.asList(messages));
        }

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
