package com.marklogic.spark.writer.file;

import com.marklogic.spark.Options;
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
        String compression = this.properties.get(Options.WRITE_FILES_COMPRESSION);
        return "zip".equalsIgnoreCase(compression) ?
            new ZipFileWriter(properties, hadoopConfiguration, partitionId) :
            new DocumentFileWriter(properties, hadoopConfiguration);
    }
}
