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

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import java.io.Serializable;

class MarkLogicWrite implements BatchWrite, StreamingWrite, Serializable {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicWrite.class);
    final static long serialVersionUID = 1;

    private WriteContext writeContext;

    MarkLogicWrite(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        logger.info("Number of partitions: {}", info.numPartitions());
        return new MarkLogicDataWriterFactory(writeContext);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0) {
            logger.info("Commit messages received: {}", Arrays.asList(messages));
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        if (messages != null && messages.length > 0) {
            logger.error("Abort messages received: {}", Arrays.asList(messages));
        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new MarkLogicDataWriterFactory(writeContext);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        logger.info("EpochId for commit is "+ epochId);
        if (messages != null && messages.length > 0) {
            logger.info("Commit messages received: {}", Arrays.asList(messages));
        }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        logger.info("EpochId for abort is "+ epochId);
        if (messages != null && messages.length > 0) {
            logger.info("Abort messages received: {}", Arrays.asList(messages));
        }
    }
}