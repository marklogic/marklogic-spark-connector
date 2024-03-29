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
        final boolean isZip = "zip".equalsIgnoreCase(compression);
        final boolean isGzip = "gzip".equalsIgnoreCase(compression);

        String aggregateXmlElement = this.properties.get(Options.READ_AGGREGATES_XML_ELEMENT);
        if (aggregateXmlElement != null && !aggregateXmlElement.trim().isEmpty()) {
            if (isZip) {
                return new ZipAggregateXMLFileReader(filePartition, properties, hadoopConfiguration);
            } else if (isGzip) {
                return new GZIPAggregateXMLFileReader(filePartition, properties, hadoopConfiguration);
            }
            return new AggregateXMLFileReader(filePartition, properties, hadoopConfiguration);
        } else if (isZip) {
            return new ZipFileReader(filePartition, hadoopConfiguration);
        } else if (isGzip) {
            return new GZIPFileReader(filePartition, hadoopConfiguration);
        }
        throw new ConnectorException("Only zip and gzip files supported, more to come before 2.2.0 release.");
    }
}
