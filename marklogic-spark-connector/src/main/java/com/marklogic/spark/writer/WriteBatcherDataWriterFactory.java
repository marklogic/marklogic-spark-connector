/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.MarkLogicServerException;
import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.util.SerializableConfiguration;

class WriteBatcherDataWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private final WriteContext writeContext;
    private final SerializableConfiguration hadoopConfiguration;

    WriteBatcherDataWriterFactory(WriteContext writeContext, SerializableConfiguration hadoopConfiguration) {
        this.writeContext = writeContext;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        try {
            return new WriteBatcherDataWriter(writeContext, hadoopConfiguration, partitionId);
        } catch (MarkLogicServerException ex) {
            // MarkLogicServerException wraps a non-serializable FailedRequest object. Spark must be able to
            // serialize a task failure in order to report it back to the driver; if it cannot, the task failure
            // can be silently dropped, causing the Spark job to hang indefinitely instead of failing (observed
            // with Spark 4.2.0). So the exception is wrapped in a ConnectorException with just the message,
            // ensuring the exception that propagates to Spark can always be serialized.
            throw new ConnectorException(ex.getMessage());
        }
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createWriter(partitionId, taskId);
    }
}
