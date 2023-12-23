package com.marklogic.spark.reader.file;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.SerializableConfiguration;

class FilePartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private SerializableConfiguration hadoopConfiguration;

    FilePartitionReaderFactory(SerializableConfiguration hadoopConfiguration) {
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new ZipFileReader((FilePartition) partition, hadoopConfiguration);
    }
}
