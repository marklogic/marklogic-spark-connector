package com.marklogic.spark.reader.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.util.SerializableConfiguration;

class FileBatch implements Batch {

    private PartitioningAwareFileIndex fileIndex;

    FileBatch(PartitioningAwareFileIndex fileIndex) {
        this.fileIndex = fileIndex;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String[] files = fileIndex.inputFiles();
        InputPartition[] result = new InputPartition[files.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new FilePartition(files[i]);
        }
        return result;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        // This config is needed to resolve file paths. This is our last chance to access it and provide a serialized
        // version to the factory, which must be serializable itself.
        Configuration config = SparkSession.active().sparkContext().hadoopConfiguration();
        return new FilePartitionReaderFactory(new SerializableConfiguration(config));
    }
}
