package com.marklogic.spark.writer.file;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class DocumentFileWriterFactory implements DataWriterFactory {

    static final long serialVersionUID = 1;

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;

    DocumentFileWriterFactory(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new DocumentFileWriter(properties, hadoopConfiguration);
    }
}
