/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.customcode;

import com.marklogic.spark.Util;
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
        if (Util.isWriteWithUrisOperation(customCodeContext.getProperties())) {
            return new UrisWriter(customCodeContext);
        }
        return new CustomCodeWriter(customCodeContext);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new CustomCodeWriter(customCodeContext);
    }
}
