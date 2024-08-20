/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

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
        return new WriteBatcherDataWriter(writeContext, hadoopConfiguration, partitionId);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createWriter(partitionId, taskId);
    }
}
