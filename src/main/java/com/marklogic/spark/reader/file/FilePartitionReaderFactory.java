package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class FilePartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;

    FilePartitionReaderFactory(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        FilePartition filePartition = (FilePartition) partition;
        String compression = this.properties.get(Options.READ_FILES_COMPRESSION);
        if ("zip".equalsIgnoreCase(compression)) {
            return new ZipFileReader(filePartition, hadoopConfiguration);
        } else if ("gzip".equalsIgnoreCase(compression)) {
            return new GZIPFileReader(filePartition, hadoopConfiguration);
        } else if (this.properties.get(Options.READ_AGGREGATES_XML_ELEMENT) != null) {
            return new AggregateXMLFileReader(filePartition, properties, hadoopConfiguration);
        }
        throw new ConnectorException("Only zip and gzip files supported, more to come before 2.2.0 release.");
    }
}
