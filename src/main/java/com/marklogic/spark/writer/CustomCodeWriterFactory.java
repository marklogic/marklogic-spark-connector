package com.marklogic.spark.writer;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

class CustomCodeWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private CustomCodeContext customCodeContext;

    CustomCodeWriterFactory(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new CustomCodeWriter(customCodeContext, partitionId, taskId, 0l);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new CustomCodeWriter(customCodeContext, partitionId, taskId, epochId);
    }
}
