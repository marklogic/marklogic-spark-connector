/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.marklogic.spark.reader.customcode.CustomCodeContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

public class CustomCodeWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private CustomCodeContext customCodeContext;

    public CustomCodeWriterFactory(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new CustomCodeWriter(customCodeContext, partitionId, taskId);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new CustomCodeWriter(customCodeContext, partitionId, taskId);
    }
}
