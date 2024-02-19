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
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Consumer;

public class MarkLogicWrite implements BatchWrite, StreamingWrite {

    private static final Logger logger = LoggerFactory.getLogger("com.marklogic.spark");

    private WriteContext writeContext;

    // Used solely for testing. Will never be populated in a real world scenario.
    private static Consumer<Integer> successCountConsumer;
    private static Consumer<Integer> failureCountConsumer;

    public static void setSuccessCountConsumer(Consumer<Integer> consumer) {
        successCountConsumer = consumer;
    }

    public static void setFailureCountConsumer(Consumer<Integer> consumer) {
        failureCountConsumer = consumer;
    }

    MarkLogicWrite(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public boolean useCommitCoordinator() {
        return BatchWrite.super.useCommitCoordinator();
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        Util.MAIN_LOGGER.info("Number of partitions: {}", info.numPartitions());
        return (DataWriterFactory) determineWriterFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit messages received: {}", Arrays.asList(messages));
            }
            int successCount = 0;
            int failureCount = 0;
            for (WriterCommitMessage message : messages) {
                CommitMessage msg = (CommitMessage)message;
                successCount += msg.getSuccessItemCount();
                failureCount += msg.getFailedItemCount();
            }
            if (successCountConsumer != null) {
                successCountConsumer.accept(successCount);
            }
            if (failureCountConsumer != null) {
                failureCountConsumer.accept(failureCount);
            }
            if (Util.MAIN_LOGGER.isInfoEnabled()) {
                Util.MAIN_LOGGER.info("Success count: {}", successCount);
            }
            if (failureCount > 0) {
                Util.MAIN_LOGGER.error("Failure count: {}", failureCount);
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
}
