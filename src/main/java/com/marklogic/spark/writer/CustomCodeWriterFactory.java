package com.marklogic.spark.writer;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

class CustomCodeWriterFactory implements DataWriterFactory {

    private CustomCodeContext customCodeContext;

    CustomCodeWriterFactory(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new CustomCodeWriter(customCodeContext);
    }
}
