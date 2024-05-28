package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class FileBatch implements Batch {

    private final Map<String, String> properties;
    private final PartitioningAwareFileIndex fileIndex;

    FileBatch(Map<String, String> properties, PartitioningAwareFileIndex fileIndex) {
        this.properties = properties;
        this.fileIndex = fileIndex;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String[] inputFiles = fileIndex.inputFiles();
        int numPartitions = inputFiles.length;
        if (properties.containsKey(Options.READ_NUM_PARTITIONS)) {
            String value = properties.get(Options.READ_NUM_PARTITIONS);
            try {
                numPartitions = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new ConnectorException(String.format("Invalid value for number of partitions: %s", value));
            }
        }
        return FileUtil.makeFilePartitions(inputFiles, numPartitions);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        // This config is needed to resolve file paths. This is our last chance to access it and provide a serialized
        // version to the factory, which must be serializable itself.
        Configuration config = SparkSession.active().sparkContext().hadoopConfiguration();
        FileContext fileContext = new FileContext(properties, new SerializableConfiguration(config));
        return new FilePartitionReaderFactory(fileContext);
    }
}
